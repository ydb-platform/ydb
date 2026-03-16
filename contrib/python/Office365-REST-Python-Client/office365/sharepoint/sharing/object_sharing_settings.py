from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.object_sharing_information import (
    ObjectSharingInformation,
)
from office365.sharepoint.sharing.permission_information import (
    SharingPermissionInformation,
)
from office365.sharepoint.sharing.sharepoint_sharing_settings import (
    SharePointSharingSettings,
)


class ObjectSharingSettings(Entity):
    """This class contains the information necessary to read and change the sharing status of a SharePoint object.
    It also contains a reference to SharePoint specific settings denoted by "SharePointSettings".
    """

    @property
    def web_url(self):
        # type: () -> Optional[str]
        """The URL pointing to the containing SP.Web object."""
        return self.properties.get("WebUrl", None)

    @property
    def access_request_mode(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the sharing context operates under the access request mode."""
        return self.properties.get("AccessRequestMode", None)

    @property
    def block_people_picker_and_sharing(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the current user can use the People Picker to do any sharing."""
        return self.properties.get("BlockPeoplePickerAndSharing", None)

    @property
    def can_current_user_manage_organization_readonly_link(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the current user can create or disable an organization View link."""
        return self.properties.get("CanCurrentUserManageOrganizationReadonlyLink", None)

    @property
    def can_current_user_manage_organization_read_write_link(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the current user can create or disable an organization Edit link."""
        return self.properties.get(
            "CanCurrentUserManageOrganizationReadWriteLink", None
        )

    @property
    def can_current_user_manage_readonly_link(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the current user can create or disable an anonymous View link."""
        return self.properties.get("CanCurrentUserManageReadonlyLink", None)

    @property
    def can_send_email(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether email invitations can be sent."""
        return self.properties.get("CanSendEmail", None)

    @property
    def can_send_link(self):
        # type: () -> Optional[bool]
        """Boolean indicating whether the current user can make use of Share-By-Link."""
        return self.properties.get("CanSendLink", None)

    @property
    def is_user_site_admin(self):
        # type: () -> Optional[bool]
        """Boolean that indicates whether or not the current user is a site collection administrator"""
        return self.properties.get("IsUserSiteAdmin", None)

    @property
    def list_id(self):
        # type: () -> Optional[str]
        """The unique ID of the parent list (if applicable)."""
        return self.properties.get("ListId", None)

    @property
    def roles(self):
        """
        A dictionary object that lists the display name and the id of the SharePoint regular roles.
        """
        return self.properties.get("Roles", None)

    @property
    def object_sharing_information(self):
        """
        Contains information about the sharing state of a shareable object.
        """
        return self.properties.get(
            "ObjectSharingInformation",
            ObjectSharingInformation(
                self.context,
                ResourcePath("ObjectSharingInformation", self.resource_path),
            ),
        )

    @property
    def sharepoint_settings(self):
        """An object that contains the SharePoint UI specific sharing settings."""
        return self.properties.get(
            "SharePointSettings",
            SharePointSharingSettings(
                self.context, ResourcePath("SharePointSettings", self.resource_path)
            ),
        )

    @property
    def sharing_permissions(self):
        """A list of SharingPermissionInformation objects that can be used to share."""
        return self.properties.get(
            "SharingPermissions",
            SharingPermissionInformation(
                self.context, ResourcePath("SharingPermissions", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "ObjectSharingInformation": self.object_sharing_information,
                "SharePointSettings": self.sharepoint_settings,
                "SharingPermissions": self.sharing_permissions,
            }
            default_value = property_mapping.get(name, None)
        return super(ObjectSharingSettings, self).get_property(name, default_value)
