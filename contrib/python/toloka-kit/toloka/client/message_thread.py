__all__ = [
    'RecipientsSelectType',
    'Folder',
    'Interlocutor',
    'MessageThread',
    'MessageThreadReply',
    'MessageThreadFolders',
    'MessageThreadCompose'
]
import datetime
from enum import unique
from typing import Dict, List, Optional

from ._converter import unstructure
from .filter import FilterCondition, FilterOr, FilterAnd
from .primitives.base import BaseTolokaObject
from ..util._codegen import attribute
from ..util._extendable_enum import ExtendableStrEnum


@unique
class RecipientsSelectType(ExtendableStrEnum):
    """The way of specifying message recipients.

    Attributes:
        DIRECT: A list of Toloker IDs.
        FILTER: A filter for selecting Tolokers.
        ALL: All Tolokers who completed any of your tasks at least once.
    """

    DIRECT = 'DIRECT'
    FILTER = 'FILTER'
    ALL = 'ALL'


@unique
class Folder(ExtendableStrEnum):
    """A folder with threads.
    """

    INBOX = 'INBOX'
    OUTBOX = 'OUTBOX'
    AUTOMATIC_NOTIFICATION = 'AUTOMATIC_NOTIFICATION'
    IMPORTANT = 'IMPORTANT'
    UNREAD = 'UNREAD'


class Interlocutor(BaseTolokaObject):
    """Information about a message sender or recipient.

    Attributes:
        id: The ID of the sender or recipient.
        role: The role in Toloka.
        myself: A flag that is set to `True` when the ID is yours.
    """

    @unique
    class InterlocutorRole(ExtendableStrEnum):
        """A role in Toloka.

        Attributes:
            USER: A Toloker.
            REQUESTER: A Requester.
            ADMINISTRATOR: An administrator.
            SYSTEM: The Toloka itself. This role is used for messages that are sent automatically.
        """

        USER = 'USER'
        REQUESTER = 'REQUESTER'
        ADMINISTRATOR = 'ADMINISTRATOR'
        SYSTEM = 'SYSTEM'

    id: str
    role: InterlocutorRole = attribute(autocast=True)
    myself: bool


class MessageThread(BaseTolokaObject):
    """A message thread.

    A message thread is created when you send a new message. Then responses are placed to the thread.
    Until the first response is received the message thread is in the `UNREAD` folder.

    If the message has several recipients then a message thread is created for each recipient when they responds.

    Attributes:
        id: The ID of the thread.
        topic: The message thread title. `topic` is a dictionary with two letter language codes as keys.
        interlocutors_inlined: The way of accessing information about the sender and recipients:
            * `True` — Information is available in the `interlocutors` list.
            * `False` — Information is available on a separate request.
        interlocutors: A list with information about the sender and recipients, sorted by IDs.
        messages_inlined: The way of accessing messages:
            * `True` — The messages are in the `messages` list.
            * `False` — The messages are available on a separate request.
        messages: A list with thread messages. The list is sorted by creation date: newer messages come first.
        meta: Thread meta information.
        answerable:
            * `True` — Tolokers can respond to your messages.
            * `False` — Tolokers can't respond to your messages.
        folders: Folders containing the thread.
        compose_details: The details of selecting recipients. This information is provided if the first message in the thread was yours.
        created: The date and time when the first message in the thread was created.
    """

    class ComposeDetails(BaseTolokaObject):
        """The details of selecting recipients.

        Attributes:
            recipients_select_type: The way of specifying message recipients.
            recipients_ids: A list of Toloker IDs. It is filled if `recipients_select_type` is `DIRECT`.
            recipients_filter: A filter for selecting Tolokers. It is set if `recipients_select_type` is `FILTER`.
        """

        recipients_select_type: RecipientsSelectType = attribute(autocast=True)
        recipients_ids: List[str]
        recipients_filter: FilterCondition

    class Meta(BaseTolokaObject):
        pool_id: str
        project_id: str
        assignment_id: str

    class Message(BaseTolokaObject):
        """A message in a thread.

        Attributes:
            text: The message text. The `text` is a dictionary with two letter language codes as keys.
            from_: Information about the sender.
            created: The date and time when the message was created.
        """

        text: Dict[str, str]
        from_: Interlocutor = attribute(origin='from')
        created: datetime.datetime

    id: str
    topic: Dict[str, str]
    interlocutors_inlined: bool
    interlocutors: List[Interlocutor]
    messages_inlined: bool
    messages: List[Message]

    meta: Meta
    answerable: bool

    folders: List[Folder]
    compose_details: ComposeDetails

    created: datetime.datetime


class MessageThreadReply(BaseTolokaObject):
    """A reply in a message thread.

    Attributes:
        text: A dictionary with the message text.
            Two letter language code is a key in the dictionary. The message in a preferred Toloker's language is sent, if available.
    """

    text: Dict[str, str]


class MessageThreadFolders(BaseTolokaObject):
    """Folders for a message thread.

    Attributes:
        folders: A list of folders for a message thread.
    """

    folders: List[Folder]


class MessageThreadCompose(BaseTolokaObject):
    """Parameters for creating a message thread with the first message.

    The `topic` and `text` parameters are dictionaries.
    Two letter language code is a key in the dictionaries. If available, the text in a Toloker preferred language is used.

    Attributes:
        recipients_select_type: The way of specifying message recipients.
        topic: The message thread title.
        text: The message text.
        answerable:
            * `True` — Tolokers can respond to your messages.
            * `False` — Tolokers can't respond to your messages.
        recipients_ids: A list of Toloker IDs. It is filled if `recipients_select_type` is `DIRECT`.
        recipients_filter: A filter for selecting Tolokers. It is set if `recipients_select_type` is `FILTER`.
    """

    recipients_select_type: RecipientsSelectType = attribute(autocast=True)
    topic: Dict[str, str]
    text: Dict[str, str]
    answerable: bool
    recipients_ids: List[str]
    recipients_filter: FilterCondition

    def unstructure(self) -> Optional[dict]:
        self_unstructured_dict = super().unstructure()
        if self.recipients_filter is not None and not isinstance(self.recipients_filter, (FilterOr, FilterAnd)):
            self_unstructured_dict['recipients_filter'] = unstructure(FilterAnd([self.recipients_filter]))
        return self_unstructured_dict
