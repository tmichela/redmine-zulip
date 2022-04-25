from argparse import ArgumentParser
from datetime import datetime
from functools import lru_cache
from multiprocessing.dummy import Pool
from pathlib import Path
from textwrap import dedent
from threading import Lock
from typing import Dict, List, Set, Union

import atoma
from atoma.atom import AtomEntry
import dataset
from loguru import logger as log
from redminelib import Redmine
import requests
import toml
import zulip

from .utils import indent, to_md


RESOLVED_TOPIC_PREFIX = b'\xe2\x9c\x94 '.decode('utf8')  # = '✔ '


class Publisher:
    def __init__(self, configuration: Union[str, Path]):
        """Monitor new issues on Redmine and publish to Zulip

        configuration:
            toml configuration file
        """
        conf = toml.load(configuration)

        # logging
        if 'LOGGING' in conf and 'file' in conf['LOGGING']:
            log.add(
                conf['LOGGING']['file'],
                level=conf['LOGGING'].get('level', 'DEBUG'),
                rotation=conf['LOGGING'].get('rotation'),
                retention=conf['LOGGING'].get('retention')
            )

        # database connection
        db_path = conf['DATABASE']['sql3_file']
        self._db = dataset.connect(
            db_path, engine_kwargs={"connect_args": {"check_same_thread": False}})
        self.issues = self._db['issues']
        self.lock = Lock()

        self.zulip = zulip.Client(config_file=conf['ZULIP']['bot'])
        self.stream = conf['ZULIP']['stream']

        self.redmine = Redmine(
            conf['REDMINE']['url'],
            key=conf['REDMINE']['token'],
            version=conf['REDMINE'].get('version', '5.0.0')
        )
        self.feed = conf['REDMINE']['rss_feed']

        # options
        if 'OPTIONS' in conf:
            self.discard = float(conf['OPTIONS'].get('discard_closed', 7))
            self.remind = float(conf['OPTIONS'].get('remind_open', 0))

    @staticmethod
    def format_topic(issue: dict, resolved=False) -> str:
        topic = f'Issue #{issue["task_id"]} - {issue["status_name"]}'
        if resolved:
            topic = f'{RESOLVED_TOPIC_PREFIX}{topic}'
        return topic

    def run(self):
        self.poll()
        self.track()
        log.info('done.')

    def poll(self):
        """Read issues from ``self.feed`` and add new ones to our local db

        For each new ticket:
        - add to the database for tracking
        - create a new topic on Zulip (in self.stream)
        - track state (update topic title)
        - track new messages and attachments on redmine and post on Zulip
        """

        log.info(f'Polling Redmine for new issues (publishing to {self.stream})')
        # get new issues
        issues = self._get_feed()

        new_issues = 0
        for issue in reversed(issues):
            # publish and track
            url = issue.id_

            if self.issues.find_one(url=url):
                continue  # issue already tracked

            new_issues += 1

            info = {
                'url': url,
                'task_id': int(Path(url).stem),
                'author': issue.authors[0].name,
                'title': issue.title.value,
            }
            issue = self.redmine.issue.get(info['task_id'])
            assert issue.id == info['task_id']
            info['status_name'] = issue.status.name
            info['status_id'] = issue.status.id
            info['journals'] = str([])
            info['updated'] = datetime.now()

            log.info(f'INSERT new issue:\n{info}')

            # write issue (+ journals) to zulip
            self._publish_issue(info, issue.description)
            self._publish_journal(info, issue)
            self._publish_attachment(info, issue)

        log.info(f'Found {new_issues} new issues')

    def track(self):
        """Update open tickets

        For each ticket tracked in our database, publish new messages and attachments
        """
        log.info(f'tracking {len(self.issues)} issues')
        Pool().map(self._track, [(n, issue) for n, issue in enumerate(self.issues)])

        # force reloading the list of topics to catch state changes
        self.zulip_topics.cache_clear()
        for issue in self.issues:
            self._maybe_resolve_topic(issue)

    def _track(self, data):
        n, issue = data

        # log.debug(f'{n}/{len(self.issues)} - {issue}')
        ticket = self.redmine.issue.get(issue['task_id'])

        # check for new journal and attachments: add message per entry
        self._publish_journal(issue, ticket)
        self._publish_attachment(issue, ticket)

        if ticket.status.id != issue['status_id']:
            # check for status: update the topic title
            self._update_status(issue, ticket)
            issue = self.issues.find_one(task_id=issue['task_id'])

        # close ticket
        last_update = issue.get('updated')
        if last_update is None:
            last_update = datetime.now()
            data = {'task_id': issue['task_id'],
                    'updated': last_update}
            with self.lock:
                self.issues.update(data, ['task_id'])

        # send reminder on open ticket
        if (
                ticket.status.name != 'Closed' and
                self.remind and
                (datetime.now() - last_update).days >= self.remind
        ):
            log.info(f'Ticket {ticket.id} inactive for more than {self.remind} days, '
                     'sending reminder')
            self.send(issue,
                      "It's been quiet for a while here :eyes:\ndo we have any update?")
            with self.lock:
                self.issues.update(
                    {'task_id': issue['task_id'], 'updated': datetime.now()},
                    ['task_id']
                )

        if (
                ticket.status.name == 'Closed' and
                (datetime.now() - last_update).days >= self.discard
        ):
            # ticket is closed, remove from DB
            log.info(f'ticket {ticket.id} closed and inactive for more than 7 days, stop'
                     'tracking')
            with self.lock:
                self.issues.delete(task_id=ticket.id)

    def _get_feed(self) -> List[AtomEntry]:
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
            {indent(to_md(description))}
            ```
            """)

        self.send(issue, content)
        # update database
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

            url = f'{self.redmine.url}/issues/{issue["task_id"]}#change-{journal.id}'
            msg = (
                f'**{journal.user.name}** [said]({url}):\n'
                f'```quote\n{to_md(journal.notes)}\n```'
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
        with self.lock:
            self.issues.update(data, ['task_id'])

    def _publish_attachment(self, issue, ticket):
        known_attachments = eval(issue.get('attachments', '[]') or '[]')
        new_attachments = []
        for attachment in ticket.attachments:
            uri = None

            if attachment.id in known_attachments:
                continue
            elif not hasattr(attachment, 'content_type'):
                pass
            elif (
                    attachment.content_type == 'application/octet-stream' and
                    attachment.filename.endswith('.eml')
            ):
                new_attachments.append(attachment.id)
                continue
            elif 'image' in attachment.content_type:
                res = self.upload_attachment(attachment)
                uri = res['uri']

            # publish message
            msg = (f'New attachment from **{attachment.author}**: '
                   f'[{attachment.filename}]({uri or attachment.content_url})')
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
        with self.lock:
            self.issues.update(data, ['task_id'])

    def upload_attachment(self, attachment):
        """Download attachment from Redmine and upload it on Zulip

        only publish images, other attachments are links to redmine
        """
        f = self.redmine.file.get(attachment.id)
        fpath = f.download(savepath='/tmp/')
        log.info(f"Redmine download file to: {fpath}")

        with open(fpath, 'rb') as f:
            # upload image to zulip
            result = self.zulip.call_endpoint(
                'user_uploads',
                method='POST',
                files=[f],
            )
        if result['result'] != 'success':
            log.info(f"Failed uploading file to zulip:\n{result}")
            return {'uri': None}

        return result

    def send(self, issue, content):
        """Send a message to zulip

        issue: dict
            issue informations, used to format the topic name
        content: str
            message content to publish, markdown formatted str
        """
        topic = self.format_topic(issue)
        resolved_topic = self.format_topic(issue, resolved=True)
        if resolved_topic in self.zulip_topic_names():
            topic = resolved_topic

        log.info(f'sending message to: {topic}@{self.stream}')
        reply = self.zulip.send_message({
            "type": "stream",
            "to": self.stream,
            "topic": topic,
            "content": content
        })

        if reply['result'] != 'success':
            log.info(f'{reply}\ncontent:\n{content}')

    @lru_cache()
    def zulip_topics(self) -> List[Dict]:
        """Returns all topics in self.stream
        """
        stream = self.zulip.get_stream_id(self.stream)
        stream = self.zulip.get_stream_topics(stream['stream_id'])
        return [s for s in stream['topics']]

    def zulip_topic_names(self) -> Set[str]:
        """Returns all topic names in self.stream
        """
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
            old_topic_resolved = f'{RESOLVED_TOPIC_PREFIX}{old_topic}'
            new_topic = f'Issue #{issue["task_id"]} - {ticket.status.name}'

            for topic in self.zulip_topics():
                name = topic['name']
                if name == old_topic:
                    break
                if name == old_topic_resolved:
                    new_topic = f'{RESOLVED_TOPIC_PREFIX}{new_topic}'
                    break
            else:
                log.warning(f'topic not found on zulip stream: {old_topic}')
                return

            # rename zulip topic with the new status
            res = self.zulip.update_message({
                'message_id': topic['max_id'],
                'topic': new_topic,
                'propagate_mode': 'change_all',
                'send_notification_to_old_thread': False,
                'send_notification_to_new_thread': False,
            })
            log.info(f'Update status for issue #{issue["task_id"]}: '
                     f'{issue["status_name"]} -> {ticket.status.name}\n'
                     f'{res}')

            # update DB entry
            data = {'task_id': ticket.id,
                    'status_id': ticket.status.id,
                    'status_name': ticket.status.name,
                    'updated': datetime.now()}
            with self.lock:
                self.issues.update(data, ['task_id'])

    def _maybe_resolve_topic(self, issue):
        title = self.format_topic(issue)
        resolved_title = self.format_topic(issue, resolved=True)

        for topic in self.zulip_topics():
            if topic['name'] == title and issue['status_name'] == 'Closed':
                res = self.zulip.update_message({
                    'message_id': topic['max_id'],
                    'topic': resolved_title,
                    'propagate_mode': 'change_all',
                    'send_notification_to_old_thread': False,
                })
                log.info(f'resolved: {title}\n{res}')
                break
            elif topic['name'] == resolved_title and issue['status_name'] != 'Closed':
                res = self.zulip.update_message({
                    'message_id': topic['max_id'],
                    'topic': title,
                    'propagate_mode': 'change_all',
                    'send_notification_to_old_thread': False,
                })
                log.info(f'un-resolved: {resolved_title}\n{res}')
                break


def main(argv=None):
    import os
    os.environ.setdefault('PYPANDOC_PANDOC', '/usr/bin/pandoc')

    ap = ArgumentParser('redmine-zulip-publisher',
                        description='Publish Redmine issues to Zulip')
    ap.add_argument('config', help='toml configuration file')
    args = ap.parse_args()

    publisher = Publisher(args.config)
    publisher.run()
