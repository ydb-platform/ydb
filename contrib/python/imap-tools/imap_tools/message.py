import re
import email
import base64
import imaplib
import datetime
from itertools import chain
from functools import cached_property
from email.header import decode_header
from email.message import _parseparam, _unquotevalue  # noqa
from typing import Tuple, Dict, Optional, List

from .utils import decode_value, parse_email_addresses, parse_email_date, EmailAddress, replace_html_ct_charset
from .consts import UID_PATTERN, CODECS_OFFICIAL_REPLACEMENT_CHAR


class MailMessage:
    """The email message"""

    def __init__(self, fetch_data: list) -> None:
        raw_message_data, raw_uid_data, raw_flag_data = self._get_message_data_parts(fetch_data)
        self._raw_uid_data = raw_uid_data
        self._raw_flag_data = raw_flag_data
        self.obj = email.message_from_bytes(raw_message_data)

    @classmethod
    def from_bytes(cls, raw_message_data: bytes):
        """Alternative constructor"""
        return cls([(b'', raw_message_data)])

    def __str__(self):
        repl = '[\t\n\r\f\v]'
        return f'{self.date}, {re.sub(repl, "", self.from_)}, {re.sub(repl, "", self.subject)}'

    @staticmethod
    def _get_message_data_parts(fetch_data: list) -> (bytes, bytes, List[bytes]):
        """
        :param fetch_data: Message object model
        :returns (raw_message_data: bytes, raw_uid_data: bytes, raw_flag_data: [bytes])
        *Elements may contain byte strings in any order, like: b'4517 (FLAGS (\\Recent NonJunk))'
        """
        raw_message_data = b''
        raw_uid_data = b''
        raw_flag_data = []
        for fetch_item in fetch_data:
            # flags
            if type(fetch_item) is bytes:
                raw_flag_data.append(fetch_item)
            # data, uid
            if type(fetch_item) is tuple:
                raw_uid_data = fetch_item[0]
                raw_flag_data.append(fetch_item[0])  # may contains flags (google, dovecot)
                raw_message_data = fetch_item[1]
        return raw_message_data, raw_uid_data, raw_flag_data

    @cached_property
    def uid(self) -> Optional[str]:
        """Message UID"""
        # _raw_uid_data - zimbra, yandex, gmail, gmx
        # _raw_flag_data - mail.ru, ms exchange server
        for raw_uid_item in [self._raw_uid_data] + self._raw_flag_data:
            uid_match = re.search(UID_PATTERN, raw_uid_item.decode())
            if uid_match:
                return uid_match.group('uid')
        return None

    @cached_property
    def size_rfc822(self) -> int:
        """RFC822 message size from server, bytes count, 0 if not found"""
        for raw_flag_item in self._raw_flag_data:
            size_match = re.search(r'RFC822\.SIZE\s+(?P<size>\d+)', raw_flag_item.decode())
            if size_match:
                return int(size_match.group('size'))
        return 0

    @cached_property
    def size(self) -> int:
        """Message size, bytes count"""
        return len(bytes(self.obj))

    @cached_property
    def flags(self) -> Tuple[str, ...]:
        """
        Message flags
        *This attribute will not be changed after "flag" actions
        """
        result = []
        for raw_flag_item in self._raw_flag_data:
            result.extend(imaplib.ParseFlags(raw_flag_item))
        return tuple(i.decode().strip() for i in result)  # noqa

    @cached_property
    def subject(self) -> str:
        """Message subject"""
        if 'subject' in self.obj:
            raw = self.obj['subject']
            return ''.join(decode_value(*head_part) for head_part in decode_header(raw))
        return ''

    @cached_property
    def from_values(self) -> Optional[EmailAddress]:
        """Sender (all data)"""
        result_set = parse_email_addresses(self.obj['From'] or '')
        return result_set[0] if result_set else None

    @cached_property
    def from_(self) -> str:
        """Sender email"""
        return self.from_values.email if self.from_values else ''

    @cached_property
    def to_values(self) -> Tuple[EmailAddress, ...]:
        """Recipients (all data)"""
        return tuple(chain(*(parse_email_addresses(i or '') for i in self.obj.get_all('To', []))))

    @cached_property
    def to(self) -> Tuple[str, ...]:
        """Recipients emails"""
        return tuple(i.email for i in self.to_values)

    @cached_property
    def cc_values(self) -> Tuple[EmailAddress, ...]:
        """Carbon copy (all data)"""
        return tuple(chain(*(parse_email_addresses(i or '') for i in self.obj.get_all('Cc', []))))

    @cached_property
    def cc(self) -> Tuple[str, ...]:
        """Carbon copy emails"""
        return tuple(i.email for i in self.cc_values)

    @cached_property
    def bcc_values(self) -> Tuple[EmailAddress, ...]:
        """Blind carbon copy (all data)"""
        return tuple(chain(*(parse_email_addresses(i or '') for i in self.obj.get_all('Bcc', []))))

    @cached_property
    def bcc(self) -> Tuple[str, ...]:
        """Blind carbon copy emails"""
        return tuple(i.email for i in self.bcc_values)

    @cached_property
    def reply_to_values(self) -> Tuple[EmailAddress, ...]:
        """Reply-to emails (all data)"""
        return tuple(chain(*(parse_email_addresses(i or '') for i in self.obj.get_all('Reply-To', []))))

    @cached_property
    def reply_to(self) -> Tuple[str, ...]:
        """Reply-to emails"""
        return tuple(i.email for i in self.reply_to_values)

    @cached_property
    def date_str(self) -> str:
        """Message sent date string as is"""
        return str(self.obj['Date'] or '')

    @cached_property
    def date(self) -> datetime.datetime:
        """Message sent date"""
        return parse_email_date(self.date_str)

    @cached_property
    def text(self) -> str:
        """Plain text of the mail message"""
        results = []
        for part in self.obj.walk():
            if part.get_content_maintype() == 'multipart' or part.get_filename():
                continue
            if part.get_content_type() in ('text/plain', 'text/'):
                results.append(decode_value(part.get_payload(decode=True), part.get_content_charset()))
        return ''.join(results)

    @cached_property
    def html(self) -> str:
        """HTML text of the mail message"""
        results = []
        for part in self.obj.walk():
            if part.get_content_maintype() == 'multipart' or part.get_filename():
                continue
            if part.get_content_type() == 'text/html':
                html = decode_value(part.get_payload(decode=True), part.get_content_charset())
                results.append(replace_html_ct_charset(html, 'utf-8'))
        return ''.join(results)

    @cached_property
    def headers(self) -> Dict[str, Tuple[str, ...]]:
        """
        Message headers
        Keys in result dict are in lower register (email headers are not case-sensitive)
        """
        result = {}
        for key, val in getattr(self.obj, '_headers', ()):
            result.setdefault(key.lower(), []).append(val)
        return {k: tuple(v) for k, v in result.items()}

    @cached_property
    def attachments(self) -> List['MailAttachment']:
        """
        Mail message attachments list
        :return: [MailAttachment]
        """
        results = []
        for part in self.obj.walk():
            if part.get_content_maintype() == 'multipart':  # multipart/* are containers
                continue
            if part.get('Content-ID') is None and part.get_filename() is None \
                    and part.get_content_type() != 'message/rfc822':
                continue
            results.append(MailAttachment(part))
        return results


class MailAttachment:
    """An attachment for a MailMessage"""

    def __init__(self, part) -> None:
        self.part = part

    def __str__(self):
        return f'<{self.filename} | {self.content_type} | {self.content_disposition} | {self.content_id}>'

    @cached_property
    def filename(self) -> str:
        """
        Attachment filename
        nameless cases:
            inline file (Content-Disposition = inline)
            forwarded message (Content-Type = message/rfc822)
        :return: filename
        """
        # attempt 1
        raw = self.part.get_filename() or ''
        attempt_1_filename = ''.join(decode_value(*head_part) for head_part in decode_header(raw))
        if CODECS_OFFICIAL_REPLACEMENT_CHAR not in attempt_1_filename:
            return attempt_1_filename

        # attempt 2 - non-ascii filename
        for header_name, target_param_name in (('content-disposition', 'filename'), ('content-type', 'name')):
            header_obj = self.part.get(header_name, None)
            if not header_obj:
                continue
            for header_item in decode_header(header_obj):
                if header_item[1] == 'unknown-8bit':
                    try:
                        # suppose encoded utf8
                        parsed_params = _parseparam(header_item[0].decode(errors='replace'))  # ['',]
                    except Exception:  # noqa
                        continue
                    for parsed_param_item in parsed_params:
                        try:
                            name, val = parsed_param_item.split('=', 1)
                            name = name.strip()
                            val = val.strip()
                        except ValueError:
                            # Must have been a bare attribute
                            name = parsed_param_item.strip()
                            val = ''
                        if name == target_param_name and val and CODECS_OFFICIAL_REPLACEMENT_CHAR not in val:
                            return val.strip('"')

        return attempt_1_filename

    @cached_property
    def content_id(self) -> str:
        if 'Content-ID' in self.part:
            raw = self.part['Content-ID']
            return ''.join(decode_value(*head_part) for head_part in decode_header(raw)).lstrip('<').rstrip('>')
        return ''

    @cached_property
    def content_type(self) -> str:
        return self.part.get_content_type()

    @cached_property
    def content_disposition(self) -> str:
        return self.part.get_content_disposition() or ''

    @cached_property
    def payload(self) -> bytes:
        payload = self.part.get_payload(decode=True)
        if payload:
            return payload
        # multipart payload, such as .eml (see get_payload)
        multipart_payload = self.part.get_payload()
        if isinstance(multipart_payload, list):
            for payload_item in multipart_payload:
                if hasattr(payload_item, 'as_bytes'):
                    payload_item_bytes = payload_item.as_bytes()
                    cte = str(self.part.get('content-transfer-encoding', '')).lower().strip()
                    if cte == 'base64':
                        return base64.b64decode(payload_item_bytes)
                    elif cte in ('7bit', '8bit', 'quoted-printable', 'binary', ''):
                        return payload_item_bytes  # quopri.decodestring
        # could not find payload
        return b''

    @cached_property
    def size(self) -> int:
        """Attachment size, bytes count"""
        return len(self.payload)
