import re
import sys

SHORT_MONTH_NAMES = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec')

UID_PATTERN = re.compile(r'(^|\s+|\W)UID\s+(?P<uid>\d+)')

CODECS_OFFICIAL_REPLACEMENT_CHAR = '�'

PYTHON_VERSION_MINOR = int(sys.version_info.minor)

MOVE_RESULT_TAG = ('_MOVE',)  # const delete_result part for mailbox.move result, when server have MOVE in capabilities

class MailMessageFlags:
    """
    System email message flags
    All system flags begin with "\"
    """
    SEEN = '\\Seen'
    ANSWERED = '\\Answered'
    FLAGGED = '\\Flagged'
    DELETED = '\\Deleted'
    DRAFT = '\\Draft'
    RECENT = '\\Recent'
    all = (
        SEEN, ANSWERED, FLAGGED, DELETED, DRAFT, RECENT
    )


class MailBoxFolderStatusOptions:
    """Valid mailbox folder status options"""
    MESSAGES = 'MESSAGES'
    RECENT = 'RECENT'
    UIDNEXT = 'UIDNEXT'
    UIDVALIDITY = 'UIDVALIDITY'
    UNSEEN = 'UNSEEN'
    all = (
        MESSAGES, RECENT, UIDNEXT, UIDVALIDITY, UNSEEN
    )
    description = (
        (MESSAGES, "The number of messages in the mailbox"),
        (RECENT, "The number of messages with the Recent flag set"),
        (UIDNEXT, "The next unique identifier value of the mailbox"),
        (UIDVALIDITY, "The unique identifier validity value of the mailbox"),
        (UNSEEN, "The number of messages which do not have the Seen flag set"),
    )


class SortCriteria:
    """
    Sort criteria
    https://datatracker.ietf.org/doc/html/rfc5256
    ARRIVAL - Internal date and time of the message.
        This differs from the ON criteria in SEARCH, which uses just the internal date.
    CC - [IMAP] addr-mailbox of the first "cc" address.
    DATE - Sent date and time, as described in section 2.2.
    FROM - [IMAP] addr-mailbox of the first "From" address.
    SIZE - Size of the message in octets.
    SUBJECT - Base subject text.
    TO - [IMAP] addr-mailbox of the first "To" address.
    """
    ARRIVAL_DT_ASC = 'ARRIVAL'
    CC_ASC = 'CC'
    DATE_ASC = 'DATE'
    FROM_ASC = 'FROM'
    SIZE_ASC = 'SIZE'
    SUBJECT_ASC = 'SUBJECT'
    TO_ASC = 'TO'

    ARRIVAL_DT_DESC = 'REVERSE ARRIVAL'
    CC_DESC = 'REVERSE CC'
    DATE_DESC = 'REVERSE DATE'
    FROM_DESC = 'REVERSE FROM'
    SIZE_DESC = 'REVERSE SIZE'
    SUBJECT_DESC = 'REVERSE SUBJECT'
    TO_DESC = 'REVERSE TO'

    all = (
        ARRIVAL_DT_ASC, CC_ASC, DATE_ASC, FROM_ASC, SIZE_ASC, SUBJECT_ASC, TO_ASC,
        ARRIVAL_DT_DESC, CC_DESC, DATE_DESC, FROM_DESC, SIZE_DESC, SUBJECT_DESC, TO_DESC,
    )
