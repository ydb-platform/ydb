import datetime
from typing import Optional

from office365.entity import Entity


class AttachmentBase(Entity):
    """Represents an abstract base type for an attachment. You can add related content to a todoTask in the form
    of an attachment."""

    @property
    def content_type(self):
        # type: () -> Optional[str]
        """The MIME type."""
        return self.properties.get("contentType", None)

    @property
    def last_modified_datetime(self):
        # type: () -> Optional[datetime.datetime]
        """
        The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time.
        """
        return self.properties.get("lastModifiedDateTime", datetime.datetime.min)

    @property
    def name(self):
        # type: () -> Optional[str]
        """
        The display name of the attachment. This does not need to be the actual file name.
        """
        return self.properties.get("name", None)

    @property
    def size(self):
        # type: () -> Optional[int]
        """The length of the attachment in bytes."""
        return self.properties.get("size", None)
