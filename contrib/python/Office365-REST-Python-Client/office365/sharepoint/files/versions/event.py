from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.shared_with_user import SharedWithUser


class FileVersionEvent(Entity):
    """Represents an event object happened on a Microsoft.SharePoint.SPFile."""

    @property
    def event_type(self):
        # type: () -> Optional[str]
        """Returns the type of the event."""
        return self.properties.get("EventType", None)

    @property
    def editor(self):
        # type: () -> Optional[str]
        """Returns the name of the user who initiated the event."""
        return self.properties.get("Editor", None)

    @property
    def shared_by_user(self):
        """Returns the shared by user Information in sharing action for change log."""
        return self.properties.get("SharedByUser", SharedWithUser())

    @property
    def shared_with_users(self):
        # type: () -> ClientValueCollection[SharedWithUser]
        """Returns the array of users that have been shared in sharing action for the change log."""
        return self.properties.get(
            "SharedWithUsers", ClientValueCollection(SharedWithUser)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "SharedByUser": self.shared_by_user,
                "SharedWithUsers": self.shared_with_users,
            }
            default_value = property_mapping.get(name, None)
        return super(FileVersionEvent, self).get_property(name, default_value)
