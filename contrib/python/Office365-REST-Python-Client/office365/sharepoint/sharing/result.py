from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.principal.groups.collection import GroupCollection
from office365.sharepoint.principal.groups.group import Group
from office365.sharepoint.principal.users.collection import UserCollection
from office365.sharepoint.sharing.invitation.creation_result import (
    SPInvitationCreationResult,
)
from office365.sharepoint.sharing.user_sharing_result import UserSharingResult


class SharingResult(Entity):
    """Contains properties generated as a result of sharing."""

    @property
    def url(self):
        # type: () -> Optional[str]
        """Gets the URL of the securable object being shared."""
        return self.properties.get("Url", None)

    @property
    def error_message(self):
        # type: () -> Optional[str]
        """Gets an error message about the failure if sharing was unsuccessful."""
        return self.properties.get("ErrorMessage", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Gets the name of the securable object being shared."""
        return self.properties.get("Name", None)

    @property
    def icon_url(self):
        # type: () -> Optional[str]
        """Gets a URL to an icon that represents the securable object, if one exists."""
        return self.properties.get("IconUrl", None)

    @property
    def status_code(self):
        # type: () -> Optional[int]
        """
        Gets the enumeration value which summarizes the result of the sharing operation.
        """
        return self.properties.get("StatusCode", None)

    @property
    def permissions_page_relative_url(self):
        # type: () -> Optional[str]
        """Gets the relative URL of the page that shows permissions."""
        return self.properties.get("PermissionsPageRelativeUrl", None)

    @property
    def invited_users(self):
        # type: () ->  ClientValueCollection[SPInvitationCreationResult]
        """
        Gets a list of SPInvitationCreationResult (section 3.2.5.325) objects representing the external users being
        invited to have access.
        """
        return self.properties.get(
            "InvitedUsers", ClientValueCollection(SPInvitationCreationResult)
        )

    @property
    def uniquely_permissioned_users(self):
        # type: () ->  ClientValueCollection[UserSharingResult]
        return self.properties.get(
            "UniquelyPermissionedUsers", ClientValueCollection(UserSharingResult)
        )

    @property
    def groups_shared_with(self):
        return self.properties.get(
            "GroupsSharedWith",
            GroupCollection(
                self.context, ResourcePath("GroupsSharedWith", self.resource_path)
            ),
        )

    @property
    def group_users_added_to(self):
        return self.properties.get(
            "GroupUsersAddedTo",
            Group(self.context, ResourcePath("GroupUsersAddedTo", self.resource_path)),
        )

    @property
    def users_with_access_requests(self):
        return self.properties.get(
            "UsersWithAccessRequests",
            UserCollection(
                self.context,
                ResourcePath("UsersWithAccessRequests", self.resource_path),
            ),
        )

    @property
    def users_added_to_group(self):
        # type: () ->  ClientValueCollection[UserSharingResult]
        """Gets the list of users being added to the SharePoint permissions group."""
        return self.properties.get(
            "UsersAddedToGroup", ClientValueCollection(UserSharingResult)
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "GroupsSharedWith": self.groups_shared_with,
                "GroupUsersAddedTo": self.group_users_added_to,
                "UsersAddedToGroup": self.users_added_to_group,
                "UniquelyPermissionedUsers": self.uniquely_permissioned_users,
                "UsersWithAccessRequests": self.users_with_access_requests,
            }
            default_value = property_mapping.get(name, None)
        return super(SharingResult, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(SharingResult, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if self._resource_path is None:
            if name == "Name":
                pass
                # self._resource_path = ResourcePath(value, self.parent_collection.resource_path)
        return self
