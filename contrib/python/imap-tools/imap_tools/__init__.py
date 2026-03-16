# Lib author: Vladimir Kaukin <KaukinVK@ya.ru>
# Project home page: https://github.com/ikvk/imap_tools
# Mirror: https://gitflic.ru/project/ikvk/imap-tools
# License: Apache-2.0

from .query import AND, OR, NOT, Header, UidRange, A, O, N, H, U
from .mailbox import BaseMailBox, MailBox, MailBoxUnencrypted, MailBoxStartTls
from .message import MailMessage, MailAttachment
from .folder import MailBoxFolderManager, FolderInfo
from .consts import MailMessageFlags, MailBoxFolderStatusOptions, SortCriteria
from .utils import EmailAddress
from .errors import (
    ImapToolsError, MailboxAppendError, MailboxCopyError, MailboxDeleteError, MailboxExpungeError, MailboxFetchError,
    MailboxFlagError, MailboxFolderCreateError, MailboxFolderDeleteError, MailboxFolderRenameError,
    MailboxFolderSelectError, MailboxFolderStatusError, MailboxFolderStatusValueError, MailboxFolderSubscribeError,
    MailboxLoginError, MailboxLogoutError, MailboxMoveError, MailboxNumbersError, MailboxStarttlsError,
    MailboxTaggedResponseError, MailboxUidsError, UnexpectedCommandStatusError)

__version__ = '1.11.1'

__all__ = [
    'A', 'AND', 'BaseMailBox', 'EmailAddress', 'FolderInfo', 'H', 'Header', 'ImapToolsError', 'MailAttachment',
    'MailBox', 'MailBoxFolderManager', 'MailBoxFolderStatusOptions', 'MailBoxStartTls', 'MailBoxUnencrypted',
    'MailMessage', 'MailMessageFlags', 'MailboxAppendError', 'MailboxCopyError', 'MailboxDeleteError',
    'MailboxExpungeError', 'MailboxFetchError', 'MailboxFlagError', 'MailboxFolderCreateError',
    'MailboxFolderDeleteError', 'MailboxFolderRenameError', 'MailboxFolderSelectError', 'MailboxFolderStatusError',
    'MailboxFolderStatusValueError', 'MailboxFolderSubscribeError', 'MailboxLoginError', 'MailboxLogoutError',
    'MailboxMoveError', 'MailboxNumbersError', 'MailboxStarttlsError', 'MailboxTaggedResponseError', 'MailboxUidsError',
    'N', 'NOT', 'O', 'OR', 'SortCriteria', 'U', 'UidRange', 'UnexpectedCommandStatusError', '__version__'
]
