__all__ = [
    'Attachment',
    'AssignmentAttachment'
]

import datetime
from enum import unique

from .owner import Owner
from .primitives.base import BaseTolokaObject
from ..util._docstrings import inherit_docstrings
from ..util._extendable_enum import ExtendableStrEnum


class Attachment(BaseTolokaObject, spec_enum='Type', spec_field='attachment_type'):
    """An attachment.

    Files attached to tasks by Tolokers are uploaded to Toloka.

    Attributes:
        id: The file ID.
        name: The file name.
        details: Attachment details: a pool, task, and Toloker who uploaded the file.
        created: The date and time when the file was uploaded.
        media_type: The file [MIME](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types) data type.
        owner: The owner of the attachment.
    """

    @unique
    class Type(ExtendableStrEnum):
        ASSIGNMENT_ATTACHMENT = 'ASSIGNMENT_ATTACHMENT'

    ASSIGNMENT_ATTACHMENT = Type.ASSIGNMENT_ATTACHMENT

    class Details(BaseTolokaObject):
        """Attachment details.

        Attributes:
            user_id: The ID of the Toloker who attached the file.
            assignment_id: The ID of the assignment with the file.
            pool_id: The ID of the pool.
        """

        user_id: str
        assignment_id: str
        pool_id: str

    id: str
    name: str
    details: Details
    created: datetime.datetime
    media_type: str

    owner: Owner


@inherit_docstrings
class AssignmentAttachment(Attachment, spec_value=Attachment.Type.ASSIGNMENT_ATTACHMENT):
    """An attachment to an assignment.

    Example:
        >>> attachment = toloka_client.get_attachment(attachment_id='0983459b-e26f-42f3-a5fd-6e3feee913e7')
        >>> print(attachment.id, attachment.name)
        ...
    """

    pass
