import re
import imaplib
import datetime
from collections import UserString
from typing import Optional, List, Iterable, Sequence, TypeVar, Union, Tuple, Iterator

from .message import MailMessage
from .folder import MailBoxFolderManager
from .idle import IdleManager
from .consts import UID_PATTERN, PYTHON_VERSION_MINOR, MOVE_RESULT_TAG
from .utils import clean_uids, check_command_status, chunked, encode_folder, clean_flags, check_timeout_arg_support, \
    chunked_crop, StrOrBytes
from .errors import MailboxStarttlsError, MailboxLoginError, MailboxLogoutError, MailboxNumbersError, \
    MailboxFetchError, MailboxExpungeError, MailboxDeleteError, MailboxCopyError, MailboxFlagError, \
    MailboxAppendError, MailboxUidsError, MailboxTaggedResponseError, MailboxMoveError

# Maximal line length when calling readline(). This is to prevent reading arbitrary length lines.
# 20Mb is enough for search response with about 2 000 000 message numbers
imaplib._MAXLINE = 20 * 1024 * 1024  # 20Mb

Criteria = Union[StrOrBytes, UserString]
Self = TypeVar("Self", bound="BaseMailBox")

class BaseMailBox:
    """Working with the email box"""

    email_message_class = MailMessage
    folder_manager_class = MailBoxFolderManager
    idle_manager_class = IdleManager

    def __init__(self) -> None:
        self.client = self._get_mailbox_client()
        self.folder = self.folder_manager_class(self)
        self.idle = self.idle_manager_class(self)
        self.login_result = None

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.logout()

    def _get_mailbox_client(self) -> imaplib.IMAP4:
        raise NotImplementedError

    def consume_until_tagged_response(self, tag: bytes):
        """Waiting for tagged response"""
        tagged_commands = self.client.tagged_commands
        response_set = []
        while True:
            response: bytes = self.client._get_response()  # noqa, example: b'IJDH3 OK IDLE Terminated'
            if tagged_commands[tag]:
                break
            response_set.append(response)
        result = tagged_commands.pop(tag)
        check_command_status(result, MailboxTaggedResponseError)
        return result, response_set

    def login(self: Self, username: str, password: str, initial_folder: Optional[str] = 'INBOX') -> Self:
        """Authenticate to account"""
        login_result = self.client._simple_command('LOGIN', username, self.client._quote(password))  # noqa
        check_command_status(login_result, MailboxLoginError)
        self.client.state = 'AUTH'  # logic from self.client.login
        if initial_folder is not None:
            self.folder.set(initial_folder)
        self.login_result = login_result
        return self  # return self in favor of context manager

    def login_utf8(self: Self, username: str, password: str, initial_folder: Optional[str] = 'INBOX') -> Self:
        """Authenticate to an account with a UTF-8 username and/or password"""
        # rfc2595 section 6 - PLAIN SASL mechanism
        encoded = (b"\0" + username.encode("utf8") + b"\0" + password.encode("utf8"))
        # Assumption is the server supports AUTH=PLAIN capability
        login_result = self.client.authenticate("PLAIN", lambda x: encoded)
        check_command_status(login_result, MailboxLoginError)
        if initial_folder is not None:
            self.folder.set(initial_folder)
        self.login_result = login_result
        return self

    def xoauth2(self: Self, username: str, access_token: str, initial_folder: Optional[str] = 'INBOX') -> Self:
        """Authenticate to account using OAuth 2.0 mechanism"""
        auth_string = f'user={username}\1auth=Bearer {access_token}\1\1'
        result = self.client.authenticate('XOAUTH2', lambda x: auth_string)  # noqa
        check_command_status(result, MailboxLoginError)
        if initial_folder is not None:
            self.folder.set(initial_folder)
        self.login_result = result
        return self

    def logout(self) -> tuple:
        """Informs the server that the client is done with the connection"""
        result = self.client.logout()
        check_command_status(result, MailboxLogoutError, expected='BYE')
        return result

    def numbers(self, criteria: Criteria = 'ALL', charset: str = 'US-ASCII') -> List[str]:
        """
        Search mailbox for matching message numbers in current folder (this is not uids)
        Message Sequence Number Message Attribute - to accessing messages by relative position in the mailbox,
        it also can be used in mathematical calculations, see rfc3501.
        :param criteria: message search criteria (see examples at ./doc/imap_search_criteria.txt)
        :param charset: IANA charset, indicates charset of the strings that appear in the search criteria. See rfc2978
        :return email message numbers
        """
        encoded_criteria = criteria if type(criteria) is bytes else str(criteria).encode(charset)
        search_result = self.client.search(charset, encoded_criteria)
        check_command_status(search_result, MailboxNumbersError)
        return search_result[1][0].decode().split() if search_result[1][0] else []

    def numbers_to_uids(self, numbers: List[str]) -> List[str]:
        """Get message uids by message numbers"""
        if not numbers:
            return []
        fetch_result = self.client.fetch(','.join(numbers), "(UID)")
        check_command_status(fetch_result, MailboxFetchError)
        result = []
        for raw_uid_item in fetch_result[1]:
            uid_match = re.search(UID_PATTERN, (raw_uid_item or b'').decode())
            if uid_match:
                result.append(uid_match.group('uid'))
        return result

    def uids(self, criteria: Criteria = 'ALL', charset: str = 'US-ASCII',
             sort: Optional[Union[str, Iterable[str]]] = None) -> List[str]:
        """
        Search mailbox for matching message uids in current folder
        :param criteria: message search criteria (see examples at ./doc/imap_search_criteria.txt)
        :param charset: IANA charset, indicates charset of the strings that appear in the search criteria. See rfc2978
        :param sort: criteria for sort messages on server, use SortCriteria constants. Charset arg is important for sort
        :return: email message uids
        """
        encoded_criteria = criteria if type(criteria) is bytes else str(criteria).encode(charset)
        if sort:
            sort = (sort,) if isinstance(sort, str) else sort
            uid_result = self.client.uid('SORT', f'({" ".join(sort)})', charset, encoded_criteria)
        else:
            uid_result = self.client.uid('SEARCH', 'CHARSET', charset, encoded_criteria)  # *charset are opt here
        check_command_status(uid_result, MailboxUidsError)
        return uid_result[1][0].decode().split() if uid_result[1][0] else []

    def _fetch_by_one(self, uid_list: Sequence[str], message_parts: str) -> Iterator[list]:
        for uid in uid_list:
            fetch_result = self.client.uid('fetch', uid, message_parts)
            check_command_status(fetch_result, MailboxFetchError)
            if not fetch_result[1] or fetch_result[1][0] is None:
                continue
            yield fetch_result[1]

    def _fetch_in_bulk(self, uid_list: Sequence[str], message_parts: str, reverse: bool, bulk: int) \
            -> Iterator[list]:
        if not uid_list:
            return

        if isinstance(bulk, int) and bulk >= 2:
            uid_list_seq = chunked_crop(uid_list, bulk)
        elif isinstance(bulk, bool):
            uid_list_seq = (uid_list,)
        else:
            raise ValueError('bulk arg may be bool or int >= 2')

        for uid_list_i in uid_list_seq:
            fetch_result = self.client.uid('fetch', ','.join(uid_list_i), message_parts)
            check_command_status(fetch_result, MailboxFetchError)
            if not fetch_result[1] or fetch_result[1][0] is None:
                return
            for built_fetch_item in chunked((reversed if reverse else iter)(fetch_result[1]), 2):
                yield built_fetch_item

    def fetch(self, criteria: Criteria = 'ALL', charset: str = 'US-ASCII', limit: Optional[Union[int, slice]] = None,
              mark_seen=True, reverse=False, headers_only=False, bulk: Union[bool, int] = False,
              sort: Optional[Union[str, Iterable[str]]] = None) \
            -> Iterator[MailMessage]:
        """
        Mail message generator in current folder by search criteria
        :param criteria: message search criteria (see examples at ./doc/imap_search_criteria.txt)
        :param charset: IANA charset, indicates charset of the strings that appear in the search criteria. See rfc2978
        :param limit: int | slice - limit number of read emails | slice emails range for read
                      useful for actions with a large number of messages, like "move" | paging
        :param mark_seen: mark emails as seen on fetch
        :param reverse: in order from the larger date to the smaller
        :param headers_only: get only email headers (without text, html, attachments)
        :param bulk:
            False - fetch each message separately per N commands - low memory consumption, slow
            True  - fetch all messages per 1 command - high memory consumption, fast. Fails on big bulk at server
            int - fetch messages by bulks of the specified size
        :param sort: criteria for sort messages on server, use SortCriteria constants. Charset arg is important for sort
        :return generator: MailMessage
        """
        message_parts = \
            f"(BODY{'' if mark_seen else '.PEEK'}[{'HEADER' if headers_only else ''}] UID FLAGS RFC822.SIZE)"
        limit_range = slice(0, limit) if type(limit) is int else limit or slice(None)
        assert type(limit_range) is slice
        uids = tuple((reversed if reverse else iter)(self.uids(criteria, charset, sort)))[limit_range]
        if bulk:
            message_generator = self._fetch_in_bulk(uids, message_parts, reverse, bulk)
        else:
            message_generator = self._fetch_by_one(uids, message_parts)
        for fetch_item in message_generator:
            yield self.email_message_class(fetch_item)

    def expunge(self) -> tuple:
        result = self.client.expunge()
        check_command_status(result, MailboxExpungeError)
        return result

    def delete(self, uid_list: Union[str, Iterable[str]], chunks: Optional[int] = None) \
            -> Optional[List[Tuple[tuple, tuple]]]:
        """
        Delete email messages
        Do nothing on empty uid_list
        :param uid_list: UIDs for delete
        :param chunks: Number of UIDs to process at once, to avoid server errors on large set. Proc all at once if None.
        :return: None on empty uid_list, command results otherwise
        """
        cleaned_uid_list = clean_uids(uid_list)
        if not cleaned_uid_list:
            return None
        results = []
        for cleaned_uid_list_i in chunked_crop(cleaned_uid_list, chunks):
            store_result = self.client.uid('STORE', ','.join(cleaned_uid_list_i), '+FLAGS', r'(\Deleted)')
            check_command_status(store_result, MailboxDeleteError)
            expunge_result = self.expunge()
            results.append((store_result, expunge_result))
        return results

    def copy(self, uid_list: Union[str, Iterable[str]], destination_folder: StrOrBytes, chunks: Optional[int] = None) \
            -> Optional[List[tuple]]:
        """
        Copy email messages into the specified folder.
        Do nothing on empty uid_list.
        :param uid_list: UIDs for copy
        :param destination_folder: Folder for email copies
        :param chunks: Number of UIDs to process at once, to avoid server errors on large set. Proc all at once if None.
        :return: None on empty uid_list, command results otherwise
        """
        cleaned_uid_list = clean_uids(uid_list)
        if not cleaned_uid_list:
            return None
        results = []
        for cleaned_uid_list_i in chunked_crop(cleaned_uid_list, chunks):
            copy_result = self.client.uid(
                'COPY', ','.join(cleaned_uid_list_i), encode_folder(destination_folder))  # noqa
            check_command_status(copy_result, MailboxCopyError)
            results.append(copy_result)
        return results

    def move(self, uid_list: Union[str, Iterable[str]], destination_folder: StrOrBytes, chunks: Optional[int] = None) \
            -> Optional[List[Tuple[tuple, tuple]]]:
        """
        Move email messages into the specified folder.
        Do nothing on empty uid_list.
        :param uid_list: UIDs for move
        :param destination_folder: Folder for move to
        :param chunks: Number of UIDs to process at once, to avoid server errors on large set. Proc all at once if None.
        :return: None on empty uid_list, command results otherwise
        """
        cleaned_uid_list = clean_uids(uid_list)
        if not cleaned_uid_list:
            return None
        if 'MOVE' in self.client.capabilities:
            # server side move
            results = []
            for cleaned_uid_list_i in chunked_crop(cleaned_uid_list, chunks):
                move_result = self.client.uid(
                    'MOVE', ','.join(cleaned_uid_list_i), encode_folder(destination_folder))  # noqa
                check_command_status(move_result, MailboxMoveError)
                results.append((move_result, MOVE_RESULT_TAG))
            return results
        else:
            # client side move
            results = []
            for cleaned_uid_list_i in chunked_crop(cleaned_uid_list, chunks):
                copy_result = self.copy(cleaned_uid_list_i, destination_folder)
                delete_result = self.delete(cleaned_uid_list_i)
                results.append((copy_result, delete_result))
            return results

    def flag(self, uid_list: Union[str, Iterable[str]], flag_set: Union[str, Iterable[str]], value: bool,
             chunks: Optional[int] = None) -> Optional[List[Tuple[tuple, tuple]]]:
        """
        Set/unset email flags.
        Do nothing on empty uid_list.
        System flags contains in consts.MailMessageFlags.all
        :param uid_list: UIDs for set flag
        :param flag_set: Flags for operate
        :param value: Should the flags be set: True - yes, False - no
        :param chunks: Number of UIDs to process at once, to avoid server errors on large set. Proc all at once if None.
        :return: None on empty uid_list, command results otherwise
        """
        cleaned_uid_list = clean_uids(uid_list)
        if not cleaned_uid_list:
            return None
        results = []
        for cleaned_uid_list_i in chunked_crop(cleaned_uid_list, chunks):
            store_result = self.client.uid(
                'STORE',
                ','.join(cleaned_uid_list_i),
                ('+' if value else '-') + 'FLAGS',
                f'({" ".join(clean_flags(flag_set))})'
            )
            check_command_status(store_result, MailboxFlagError)
            expunge_result = self.expunge()
            results.append((store_result, expunge_result))
        return results

    def append(self, message: Union[MailMessage, bytes],
               folder: StrOrBytes = 'INBOX',
               dt: Optional[datetime.datetime] = None,
               flag_set: Optional[Union[str, Iterable[str]]] = None) -> tuple:
        """
        Append email messages to server
        :param message: MailMessage object or bytes
        :param folder: destination folder, INBOX by default
        :param dt: email message datetime with tzinfo, now by default, imaplib.Time2Internaldate types supported
        :param flag_set: email message flags, no flags by default. System flags at consts.MailMessageFlags.all
        :return: command results
        """
        if PYTHON_VERSION_MINOR < 6:
            timezone = datetime.timezone(datetime.timedelta(hours=0))
        else:
            timezone = datetime.datetime.now().astimezone().tzinfo  # system timezone
        cleaned_flags = clean_flags(flag_set or [])
        typ, dat = self.client.append(
            encode_folder(folder),  # noqa
            f'({" ".join(cleaned_flags)})' if cleaned_flags else None,
            dt or datetime.datetime.now(timezone),  # noqa
            message if type(message) is bytes else message.obj.as_bytes()
        )
        append_result = (typ, dat)
        check_command_status(append_result, MailboxAppendError)
        return append_result


class MailBox(BaseMailBox):
    """Working with the email box through IMAP4 over SSL/TLS connection with imaplib.IMAP4_SSL"""

    def __init__(self, host='', port=993, timeout=None, keyfile=None, certfile=None, ssl_context=None) -> None:
        """
        :param host: host's name (default: localhost)
        :param port: port number
        :param timeout: timeout in seconds for the connection attempt, since python 3.9
        :param keyfile: PEM formatted file that contains your private key (deprecated)
        :param certfile: PEM formatted certificate chain file (deprecated)
        :param ssl_context: SSLContext object that contains your certificate chain and private key
        Since Python 3.9 timeout argument added
        Since Python 3.12 keyfile and certfile arguments are deprecated, ssl_context and timeout must be keyword args
        """
        check_timeout_arg_support(timeout)
        self._host = host
        self._port = port
        self._timeout = timeout
        self._keyfile = keyfile
        self._certfile = certfile
        self._ssl_context = ssl_context
        super().__init__()

    def _get_mailbox_client(self) -> imaplib.IMAP4:
        if PYTHON_VERSION_MINOR < 9:
            return imaplib.IMAP4_SSL(self._host, self._port, self._keyfile, self._certfile, self._ssl_context)  # noqa
        elif PYTHON_VERSION_MINOR < 12:
            return imaplib.IMAP4_SSL(
                self._host, self._port, self._keyfile, self._certfile, self._ssl_context, self._timeout)  # noqa
        else:
            return imaplib.IMAP4_SSL(self._host, self._port, ssl_context=self._ssl_context, timeout=self._timeout)


class MailBoxUnencrypted(BaseMailBox):
    """Working with the email box through IMAP4 without encryption. Do NOT use it on the public internet!"""

    def __init__(self, host='', port=143, timeout=None) -> None:
        """
        :param host: host's name (default: localhost)
        :param port: port number
        :param timeout: timeout in seconds for the connection attempt, since python 3.9
        """
        check_timeout_arg_support(timeout)
        self._host = host
        self._port = port
        self._timeout = timeout
        super().__init__()

    def _get_mailbox_client(self) -> imaplib.IMAP4:
        if PYTHON_VERSION_MINOR < 9:
            return imaplib.IMAP4(self._host, self._port)
        elif PYTHON_VERSION_MINOR < 12:
            return imaplib.IMAP4(self._host, self._port, self._timeout)
        else:
            return imaplib.IMAP4(self._host, self._port, timeout=self._timeout)


class MailBoxStartTls(BaseMailBox):
    """Working with the email box through IMAP4 with imaplib.IMAP4 + STARTTLS"""

    def __init__(self, host='', port=143, timeout=None, ssl_context=None) -> None:
        """
        :param host: host's name (default: localhost)
        :param port: port number
        :param timeout: timeout in seconds for the connection attempt, since python 3.9
        :param ssl_context: SSLContext object that contains your certificate chain and private key
        """
        check_timeout_arg_support(timeout)
        self._host = host
        self._port = port
        self._timeout = timeout
        self._ssl_context = ssl_context
        super().__init__()

    def _get_mailbox_client(self) -> imaplib.IMAP4:
        if self._port == 993:
            raise ValueError("Port 993 requires IMAP4_SSL. Use MailBox class for SSL/TLS connection.")
        if PYTHON_VERSION_MINOR < 9:
            client = imaplib.IMAP4(self._host, self._port)
        elif PYTHON_VERSION_MINOR < 12:
            client = imaplib.IMAP4(self._host, self._port, self._timeout)
        else:
            client = imaplib.IMAP4(self._host, self._port, timeout=self._timeout)
        result = client.starttls(self._ssl_context)
        check_command_status(result, MailboxStarttlsError)
        return client
