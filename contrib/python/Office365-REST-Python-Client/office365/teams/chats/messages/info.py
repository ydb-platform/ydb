from datetime import datetime

from office365.entity import Entity
from office365.outlook.mail.item_body import ItemBody


class ChatMessageInfo(Entity):
    """Represents a preview of a chatMessage resource. This object can only be fetched as part of a list of chats."""

    @property
    def body(self):
        """
        Body of the chatMessage. This will still contain markers for @mentions and attachments even though the
        object does not return @mentions and attachments.
        """
        return self.properties.get("body", ItemBody())

    @property
    def created_datetime(self):
        """Date time object representing the time at which message was created."""
        return self.properties.get("createdDateTime", datetime.min)
