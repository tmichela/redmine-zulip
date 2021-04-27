from datetime import datetime
from enum import Enum
import logging
import logging.config
from pathlib import Path
import re
import requests
from textwrap import dedent
from typing import Any, Dict, Optional, Union

import atoma
import dataset
import pypandoc as pandoc
from redminelib import Redmine as RedmineLib
from redminelib.exceptions import ResourceAttrError
from redminelib.resources.standard import Group, Issue, User
import toml
import zulip


logger = logging.getLogger(__name__)


def textile_to_md(text):
    text = pandoc.convert_text(text, to='gfm', format='textile')
    return re.sub(r'\\(.)', r'\1', text)


def indent(text, offset=3):
    """Indent text with offet * 4 blank spaces
    """
    def indented_lines():
        for ix, line in enumerate(text.splitlines(True)):
            if ix == 0:
                yield line
            else:
                yield '    ' * offset + line if line.strip() else line
    return ''.join(indented_lines())


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
        self._db = dataset.connect(conf['DATABASE']['sql3_file'])
        self.issues = self._db['issues']
        self.members = self._db['members']

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

        for issue in issues:
            # publish and track
            url = issue.id_

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

            logger.info('INSERT new task:\n%s', info)

            # write issue (+ journals) to zulip
            self._publish_issue(info, issue.description)
            self._publish_journal(info, issue)
            self._publish_attachment(info, issue)

    def track(self):
        """Update open tickets

        For each ticket tracked in our database, publish new messages and attachments
        """
        for issue in self.issues:
            ticket = self.redmine.get(issue['task_id'])

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
                self.issues.update(data, ['task_id'])

            if ticket.status.name == 'Closed' and (datetime.now() - last_update).days >= 7:
                # ticket is closed, remove from DB
                self.issues.delete(task_id=ticket.id)

    def _get_feed(self):
        """Get issues from rss url"""
        r = requests.get(self.feed)
        if r.status_code != requests.codes.ok:
            logger.debug(f'{r.status_code} Error: {r.reason} for url: {self.feed}')
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

            url = f'{Redmine.url}/issues/{issue["task_id"]}#change-{journal.id}'
            msg = (
                f'**{journal.user.name}** [said]({url}):\n'
                f'```quote\n{textile_to_md(journal.notes)}\n```'
            )
            self.send(issue, msg)

            new_entries.append(journal.id)
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
        logger.info("Redmine download file to: %s", fpath)

        with open(fpath, 'rb') as f:
            # upload image to zulip
            result = self.zulip.call_endpoint(
                'user_uploads',
                method='POST',
                files=[f],
            )
        if result['result'] != 'success':
            logger.info("Failed uploading file to zulip:\n%s", result)
            return {'uri': None}

        return result

    def send(self, issue, content):
        reply = self.zulip.send_message({
            "type": "stream",
            "to": self.stream,
            "topic": f'Issue #{issue["task_id"]} - {issue["status_name"]}',
            "content": content
        })
        logger.info("%s", reply)

    def _update_status(self, issue, ticket):
        """Update the issue state in the zulip topic name

        note: changing a topic name means effectively moving messages to a new topic,
        this requires a client with admin right.
        """
        # legacy: check existing topics
        reply = self.zulip.get_stream_id(self.stream)
        old_topic = f'Issue #{issue["task_id"]}'
        for topic in self.zulip.get_stream_topics(reply['stream_id'])['topics']:
            if topic['name'] == old_topic:
                break
        else:
            old_topic = f'{old_topic} - {issue["status_name"]}'

        if ticket.status.name != issue['status_name']:
            # rename zulip topic for the issue with the new status
            res = self.zulip_admin.move_topic(
                self.stream, self.stream,
                old_topic,
                f'Issue #{issue["task_id"]} - {ticket.status.name}',
                notify_old_topic=False,
                notify_new_topic=False
            )
            logger.info('Update status for issue #%s: %s -> %s',
                        issue["task_id"],
                        issue["status_name"],
                        ticket.status.name
            )
            logger.info('%s', res)

            # update DB entry
            data = {'task_id': ticket.id,
                    'status_id': ticket.status.id,
                    'status_name': ticket.status.name,
                    'updated': datetime.now()}
            self.issues.update(data, ['task_id'])


if __name__ == '__main__':
    # load configuration
    config_file = Path(__file__).parent / 'config.toml'

    p = Publisher(config_file)
    p.poll()
    p.track()
