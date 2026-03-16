from datetime import datetime

from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class AttachmentSession(Entity):
    """Represents a resource that uploads large attachments to a todoTask."""

    @property
    def content(self):
        """The content streams that are uploaded."""
        return self.properties.get("content", None)

    @property
    def expiration_datetime(self):
        """The date and time in UTC when the upload session will expire.
        The complete file must be uploaded before this expiration time is reached."""
        return self.properties.get("expirationDateTime", datetime.min)

    @property
    def next_expected_ranges(self):
        """Indicates a single value {start} that represents the location in the file where the next
        upload should begin."""
        return self.properties.get("nextExpectedRanges", StringCollection())

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "expirationDateTime": self.expiration_datetime,
                "nextExpectedRanges": self.next_expected_ranges,
            }
            default_value = property_mapping.get(name, None)
        return super(AttachmentSession, self).get_property(name, default_value)
