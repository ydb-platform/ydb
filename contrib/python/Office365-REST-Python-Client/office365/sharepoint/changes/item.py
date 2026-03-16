from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.changes.change import Change
from office365.sharepoint.contenttypes.content_type_id import ContentTypeId
from office365.sharepoint.sharing.shared_with_user import SharedWithUser


class ChangeItem(Change):
    """A change on an item."""

    @property
    def activity_type(self):
        # type: () -> Optional[str]
        """Returns activity type defined in ChangeActivityType"""
        return self.properties.get("ActivityType", None)

    @property
    def content_type_id(self):
        """Specifies an identifier for the content type"""
        return self.properties.get("ContentTypeId", ContentTypeId())

    @property
    def editor(self):
        """Specifies the editor of the changed item."""
        return self.properties.get("Editor", None)

    @property
    def editor_email_hint(self):
        """Returns the email corresponding to Editor."""
        return self.properties.get("EditorEmailHint", None)

    @property
    def editor_login_name(self):
        """Returns login name of the Editor."""
        return self.properties.get("EditorLoginName", None)

    @property
    def file_type(self):
        # type: () -> Optional[str]
        """Returns the list item’s file type."""
        return self.properties.get("FileType", None)

    @property
    def item_id(self):
        # type: () -> Optional[int]
        """Identifies the changed item."""
        return self.properties.get("ItemId", None)

    @property
    def is_recycle_bin_operation(self):
        # type: () -> Optional[bool]
        return self.properties.get("IsRecycleBinOperation", None)

    @property
    def server_relative_url(self):
        # type: () -> Optional[str]
        """Specifies the server-relative URL of the item."""
        return self.properties.get("ServerRelativeUrl", None)

    @property
    def shared_by_user(self):
        """Return the sharedBy User Information in sharing action for change log."""
        return self.properties.get("SharedByUser", SharedWithUser())

    @property
    def shared_with_users(self):
        """Returns the array of users that have been shared in sharing action for the change log."""
        return self.properties.get(
            "SharedWithUsers", ClientValueCollection(SharedWithUser)
        )

    @property
    def unique_id(self):
        # type: () -> Optional[str]
        """The Document identifier of the item."""
        return self.properties.get("UniqueId", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "ContentTypeId": self.content_type_id,
                "SharedByUser": self.shared_by_user,
                "SharedWithUsers": self.shared_with_users,
            }
            default_value = property_mapping.get(name, None)
        return super(ChangeItem, self).get_property(name, default_value)
