from typing import Optional

from office365.sharepoint.entity import Entity


class EventReceiverDefinition(Entity):
    """Abstract base class that defines general properties of an event receiver for list items, lists,
    websites, and workflows."""

    @property
    def receiver_assembly(self):
        # type: () -> Optional[str]
        """Specifies the strong name of the assembly that is used for the event receiver."""
        return self.properties.get("ReceiverAssembly", None)

    @property
    def receiver_class(self):
        # type: () -> Optional[str]
        """Specifies the strong name of the assembly that is used for the event receiver."""
        return self.properties.get("ReceiverClass", None)

    @property
    def receiver_url(self):
        # type: () -> Optional[str]
        """Gets the URL of the receiver for the event."""
        return self.properties.get("ReceiverUrl", None)
