from argparse import ArgumentParser
from datetime import datetime
from functools import lru_cache, partial, wraps
import logging
from multiprocessing.dummy import Pool
from pathlib import Path
import re
from types import resolve_bases
import requests
from textwrap import dedent
from threading import Lock
from time import sleep
from typing import Union

import atoma
import dataset
import pypandoc as pandoc
from redminelib import Redmine as RedmineLib
from redminelib.resources.standard import Issue
import toml
import zulip


RESOLVED_TOPIC_PREFIX = b'\xe2\x9c\x94 '.decode('utf8')  # '✔ '


log = logging.getLogger(__name__)


def textile_to_md(text):
    text = pandoc.convert_text(text, to='markdown_github', format='textile')
    return re.sub(r'\\(.)', r'\1', text)


def indent(text, offset=3):
    """Indent text with offset * 4 blank spaces
    """
    def indented_lines():
        for ix, line in enumerate(text.splitlines(True)):
            if ix == 0:
                yield line
            else:
                yield '    ' * offset + line if line.strip() else line
    return ''.join(indented_lines())


def retry(func=None, *, attempts=1, delay=0, exc=(Exception,)):
    """Re-execute decorated function.
    :attemps int: number of tries, default 1
    :delay float: timeout between each tries in seconds, default 0
    :exc tuple: collection of exceptions to be caugth
    """
    if func is None:
        return partial(retry, attempts=attempts, delay=delay, exc=exc)

    @wraps(func)
    def retried(*args, **kwargs):
        retry._tries[func.__name__] = 0
        for i in reversed(range(attempts)):
            retry._tries[func.__name__] += 1
            try:
                ret = func(*args, *kwargs)
            except exc:
                if i <= 0:
                    raise
                sleep(delay)
                continue
            else:
                break
        return ret

    retry._tries = {}
    return retried


def format_topic(issue: dict) -> str:
    return f'Issue #{issue["task_id"]} - {issue["status_name"]}'


class Redmine:
    def __init__(self, conf):
        self.remote = RedmineLib(conf['url'], key=conf['token'])
        self.project = self.remote.project.get(conf['project'])

    def get(self, issue: int) -> Issue:
        """Get a redmine issue from it's ID
        """
        return self.remote.issue.get(issue)


class Publisher:
    def __init__(self, configuration: Union[str, Path]):
        """Monitor new issues on Redmine and publish to Zulip

        configuration:
            toml configuration file
        """
        conf = toml.load(configuration)
        self.db_path = conf['DATABASE']['sql3_file']
        self._db = dataset.connect(self.db_path, engine_kwargs={"connect_args": {"check_same_thread": False}})
        self.issues = self._db['issues']
        self.lock = Lock()

        self.zulip = zulip.Client(config_file=conf['ZULIP']['bot'])
        self.zulip_admin = zulip.Client(config_file=conf['ZULIP']['admin'])
        self.stream = conf['ZULIP']['stream']

        self.redmine = Redmine(conf['REDMINE'])
        self.feed = conf['REDMINE']['rss_feed']

    def poll(self):
        """Read issues from ``self.feed`` and add new ones to our local db

        For each new ticket:
        - add to the database for tracking
        - create a new topic on Zulip (in self.stream)
        - track state (update topic title)
        - track new messages and attachments on redmine and post on Zulip
        """
        # get new issues
        issues = self._get_feed()

        for n, issue in enumerate(issues):
            # publish and track
            url = issue.id_

            print(f'issue {n}/{len(issues)}: {issue.title.value}')
            if self.issues.find_one(url=url):
                continue  # issue already tracked

            info = {
                'url': url,
                'task_id': int(Path(url).stem),
                'author': issue.authors[0].name,
                'title': issue.title.value,
            }
            issue = self.redmine.get(info['task_id'])
            assert issue.id == info['task_id']
            info['status_name'] = issue.status.name
            info['status_id'] = issue.status.id
            info['journals'] = str([])
            info['updated'] = datetime.now()

            log.info('INSERT new task:\n%s', info)

            # write issue (+ journals) to zulip
            self._publish_issue(info, issue.description)
            self._publish_journal(info, issue)
            self._publish_attachment(info, issue)

    def track(self):
        """Update open tickets

        For each ticket tracked in our database, publish new messages and attachments
        """
        Pool().map(self._track, [(n, issue) for n, issue in enumerate(self.issues)])

    def _track(self, data):
        n, issue = data

        db = dataset.connect(self.db_path)
        issues = db['issues']

        print(f'{n}/{len(issues)} - {issue}')
        ticket = self.redmine.get(issue['task_id'])

        # check for new journal and attachments: add message per entry
        self._publish_journal(issue, ticket)
        self._publish_attachment(issue, ticket)

        if ticket.status.id != issue['status_id']:
            # check for status: update the topic title
            self._update_status(issue, ticket)
            issue = issues.find_one(task_id=issue['task_id'])

        self._maybe_resolve_topic(issue)

        # close ticket
        last_update = issue.get('updated')
        if last_update is None:
            last_update = datetime.now()
            data = {'task_id': issue['task_id'],
                    'updated': last_update}
            issues.update(data, ['task_id'])

        print(datetime.now(), last_update)
        print((datetime.now() - last_update).days, ticket.status.name)
        if ticket.status.name == 'Closed' and (datetime.now() - last_update).days >= 7:
            # ticket is closed, remove from DB
            issues.delete(task_id=ticket.id)

    def _get_feed(self):
        """Get issues from rss url"""
        r = requests.get(self.feed)
        if r.status_code != requests.codes.ok:
            log.debug(f'{r.status_code} Error: {r.reason} for url: {self.feed}')
            return []

        return atoma.parse_atom_bytes(r.content).entries

    def _publish_issue(self, issue, description):
        content = dedent(f"""\
            **{issue['author']} opened [Issue {issue['title']}]({issue['url']})**
            ```quote
            {indent(textile_to_md(description))}
            ```
            """)

        self.send(issue, content)
        # update database
        print(issue)
        self.issues.insert(issue)

    def _publish_journal(self, issue, ticket):
        known_entries = eval(issue.get('journals', '[]') or '[]')
        new_entries = []
        for journal in ticket.journals:
            if journal.id in known_entries:
                continue
            if not hasattr(journal, 'notes'):
                continue
            if not journal.notes:
                continue

            url = f'{self.redmine.remote.url}/issues/{issue["task_id"]}#change-{journal.id}'
            msg = (
                f'**{journal.user.name}** [said]({url}):\n'
                f'```quote\n{textile_to_md(journal.notes)}\n```'
            )
            self.send(issue, msg)

            new_entries.append(journal.id)
       
        if not new_entries:
            return

        known_entries += new_entries

        # update DB entry
        data = {
            'task_id': issue['task_id'],
            'journals': str([e for e in sorted(known_entries)]),
            'updated': datetime.now()
        }
        self.issues.update(data, ['task_id'])

    def _publish_attachment(self, issue, ticket):
        known_attachments = eval(issue.get('attachments', '[]') or '[]')
        new_attachments = []
        for attachment in ticket.attachments:
            uri = None

            if attachment.id in known_attachments:
                continue
            if (
                    attachment.content_type == 'application/octet-stream' and
                    attachment.filename.endswith('.eml')
            ):
                new_attachments.append(attachment.id)
                continue
            if 'image' in attachment.content_type:
                res = self.upload_attachment(attachment)
                uri = res['uri']

            msg = f'New attachment from **{attachment.author}**: [{attachment.filename}]({uri or attachment.content_url})'
            # publish message
            self.send(issue, msg)

            new_attachments.append(attachment.id)

        if not new_attachments:
            return

        known_attachments += new_attachments
        # update database
        data = {
            'task_id': issue['task_id'],
            'attachments': str([e for e in sorted(known_attachments)]),
            'updated': datetime.now()
        }
        self.issues.update(data, ['task_id'])

    def upload_attachment(self, attachment):
        """Download attachment from Redmine and upload it on Zulip

        only publish images, other attachments are links to redmine
        """
        f = self.redmine.remote.file.get(attachment.id)
        fpath = f.download(savepath='/tmp/')
        log.info("Redmine download file to: %s", fpath)

        with open(fpath, 'rb') as f:
            # upload image to zulip
            result = self.zulip.call_endpoint(
                'user_uploads',
                method='POST',
                files=[f],
            )
        if result['result'] != 'success':
            log.info("Failed uploading file to zulip:\n%s", result)
            return {'uri': None}

        return result

    def send(self, issue, content):
        topic = format_topic(issue)
        if f'{RESOLVED_TOPIC_PREFIX}{topic}' in self.zulip_topic_names():
            topic = f'{RESOLVED_TOPIC_PREFIX}{topic}'

        reply = self.zulip.send_message({
            "type": "stream",
            "to": self.stream,
            "topic": topic,
            "content": content
        })
        log.info("%s", reply)

    @lru_cache()
    @retry(attempts=10)
    def zulip_topics(self):
        stream = self.zulip.get_stream_id(self.stream)
        stream = self.zulip.get_stream_topics(stream['stream_id'])
        return [s for s in stream['topics']]

    def zulip_topic_names(self):
        return {s['name'] for s in self.zulip_topics()}

    def _update_status(self, issue, ticket):
        """Update the issue state in the zulip topic name

        note: changing a topic name means effectively moving messages to a new topic,
        this requires a client with admin right.
        """
        if ticket.status.name != issue['status_name']:
            # legacy: check existing topics
            old_topic = f'Issue #{issue["task_id"]}'
            if old_topic not in self.zulip_topic_names():
                old_topic = f'{old_topic} - {issue["status_name"]}'

            new_topic = f'Issue #{issue["task_id"]} - {ticket.status.name}'

            # rename zulip topic for the issue with the new status
            res = self.zulip_admin.move_topic(
                self.stream, self.stream,
                old_topic,
                new_topic,
                notify_old_topic=False,
                notify_new_topic=False
            )
            log.info('Update status for issue #%s: %s -> %s',
                     issue["task_id"],
                     issue["status_name"],
                     ticket.status.name)
            log.info('%s', res)

            # update DB entry
            data = {'task_id': ticket.id,
                    'status_id': ticket.status.id,
                    'status_name': ticket.status.name,
                    'updated': datetime.now()}
            self.issues.update(data, ['task_id'])

    def _maybe_resolve_topic(self, issue):
        if issue['status_name'] != 'Closed':
            return

        title = format_topic(issue)
        for topic in self.zulip_topics():
            if topic['name'] == title:
                self.zulip.update_message({
                    'message_id': topic['max_id'],
                    'topic': f'{RESOLVED_TOPIC_PREFIX}{title}',
                    'propagate_mode': 'change_all',
                    'send_notification_to_old_thread': False,
                    # 'send_notification_to_new_thread': False,
                })
                break


def main(argv=None):
    import os
    os.environ.setdefault('PYPANDOC_PANDOC', '/usr/bin/pandoc')

    ap = ArgumentParser('redmine-zulip-publisher',
                        description='Publish Redmine issues to Zulip')
    ap.add_argument('config', help='toml configuration file')
    args = ap.parse_args()

    publisher = Publisher(args.config)
    publisher.poll()
    publisher.track()
