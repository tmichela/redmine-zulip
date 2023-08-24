from argparse import ArgumentParser
from datetime import datetime
from functools import lru_cache
from multiprocessing.dummy import Pool
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set, Union

import atoma
from atoma.atom import AtomEntry
import dataset
from html2text import html2text
from loguru import logger as log
from redminelib import Redmine
import requests
import toml
import zulip


RESOLVED_TOPIC_PREFIX = b'\xe2\x9c\x94 '.decode('utf8')  # = '✔ '
CLOSED_STATES = ('Closed', 'Resolved', 'Rejected')


def format_topic_legacy(issue):
    topic = f'Issue #{issue["task_id"]} - {issue["status_name"]}'
    resolved_topic = f'{RESOLVED_TOPIC_PREFIX}{topic}'
    return topic, resolved_topic


def format_topic(issue):
    topic = f'{issue["subject"]} - #{issue["task_id"]}'
    resolved_topic = f'{RESOLVED_TOPIC_PREFIX}{topic}'
    return topic, resolved_topic


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
            info['subject'] = issue.subject
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
            self.topic_resolution(issue)

    def _track(self, data):
        n, issue = data

        # log.debug(f'{n}/{len(self.issues)} - {issue}')
        ticket = self.redmine.issue.get(issue['task_id'])

        # legacy, add issue subject to db
        if 'subject' not in issue or issue['subject'] is None:
            issue['subject'] = ticket.subject
            with self.lock:
                self.issues.update(issue, ['task_id'])

        # rename zulip topic if it is still the older formatting
        self.rename_legacy_topic(issue)

        # check for new journal and attachments: add message per entry
        self._publish_journal(issue, ticket)
        try:
            self._publish_attachment(issue, ticket)
        except:
            import traceback
            traceback.print_exc()
    
        issue = self._update_status(issue, ticket)

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
                ticket.status.name not in CLOSED_STATES and
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
                ticket.status.name in CLOSED_STATES and
                (datetime.now() - last_update).days >= self.discard
        ):
            # ticket is closed, remove from DB
            log.info(f'ticket {ticket.id} closed and inactive for more than '
                     f'{self.discard} days, stop tracking')
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
        content = (
            f"**{issue['author']} opened [Issue {issue['title']}]({issue['url']})**\n"
            "```quote\n"
            f"{html2text(description)}\n"
            "```\n"
        )

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
                f'```quote\n{html2text(journal.notes)}\n```'
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
            elif (
                    attachment.content_type is not None and
                    'image' in attachment.content_type
            ):
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
        topic, resolved_topic = format_topic(issue)
        if resolved_topic in self.zulip_topic_names(unresolved=False):
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
        return [t for t in stream['topics']]

    def zulip_topic(self, name):
        return next((t for t in self.zulip_topics() if name.startswith(t['name'][:-3])), None)

    def zulip_topic_names(self, unresolved=True, resolved=True) -> Set[str]:
        """Returns all topic names in self.stream
        """
        return [s['name'] for s in self.zulip_topics()
                if (resolved and s['name'].startswith(RESOLVED_TOPIC_PREFIX)) or
                   (unresolved and not s['name'].startswith(RESOLVED_TOPIC_PREFIX))
        ]

    def _update_status(self, issue, ticket):
        """Update the issue state in the zulip topic name

        note: changing a topic name means effectively moving messages to a new topic,
        this requires a client with admin right.
        """
        if ticket.status.name != issue['status_name']:
            # self.send(issue, f'**Status**: *{issue["status_name"]}* -> *{ticket.status.name}*')

            # update DB entry
            data = {'task_id': ticket.id,
                    'status_id': ticket.status.id,
                    'status_name': ticket.status.name,
                    'updated': datetime.now()}
            with self.lock:
                self.issues.update(data, ['task_id'])

            return self.issues.find_one(task_id=issue['task_id'])
        return issue

    def _rename_topic(self, topic, new_topic):
        res = self.zulip.update_message({
            'message_id': self.zulip_topic(topic)['max_id'],
            'topic': new_topic,
            'propagate_mode': 'change_all',
            'send_notification_to_old_thread': False,
            'send_notification_to_new_thread': False,
        })
        log.info(f'moved topic: {topic} -> {new_topic}\n{res}')

    def rename_legacy_topic(self, issue):
        topic_legacy, resolved_topic_legacy = format_topic_legacy(issue)
        topic, resolved_topic = format_topic(issue)

        if topic_legacy in self.zulip_topic_names(resolved=False):
            self._rename_topic(topic_legacy, topic)
        elif resolved_topic_legacy in self.zulip_topic_names(unresolved=False):
            self._rename_topic(resolved_topic_legacy, resolved_topic)

    def topic_resolution(self, issue):
        topic, resolved_topic = format_topic(issue)

        # TODO what if multiple topics have the same name?
        if issue['status_name'] in CLOSED_STATES:
            # topic names are only up to xx characters :(
            if topic := next((t for t in self.zulip_topic_names(resolved=False) if topic.startswith(t[:-3])), None):
                self._rename_topic(topic, resolved_topic)
        else:
            if resolved_topic := next((t for t in self.zulip_topic_names(unresolved=False) if resolved_topic.startswith(t[:-3])), None):
                self._rename_topic(resolved_topic, topic)


def main(argv=None):
    ap = ArgumentParser('redmine-zulip-publisher',
                        description='Publish Redmine issues to Zulip')
    ap.add_argument('config', help='toml configuration file')
    args = ap.parse_args()

    publisher = Publisher(args.config)
    publisher.run()
