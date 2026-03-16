from office365.outlook.mail.attachments.attachment import Attachment
from office365.runtime.paths.resource_path import ResourcePath


class ItemAttachment(Attachment):
    """A contact, event, or message that's attached to a user event, message, or post."""

    @property
    def item(self):
        """The attached message or event."""
        from office365.outlook.item import OutlookItem

        return self.properties.get(
            "item", OutlookItem(self.context, ResourcePath("item", self.resource_path))
        )
