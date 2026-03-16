from typing import Optional

from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.sharing.abilities import SharingAbilities
from office365.sharepoint.sharing.access_request_settings import AccessRequestSettings
from office365.sharepoint.sharing.domain_restriction_settings import (
    DomainRestrictionSettings,
)
from office365.sharepoint.sharing.links.default_templates_collection import (
    SharingLinkDefaultTemplatesCollection,
)
from office365.sharepoint.sharing.permission_collection import PermissionCollection
from office365.sharepoint.sharing.picker_settings import PickerSettings


class SharingInformation(Entity):
    """Represents a response for Microsoft.SharePoint.Client.Sharing.SecurableObjectExtensions.GetSharingInformation.
    The accessRequestSettings, domainRestrictionSettings and permissionsInformation properties are not included in
    the default scalar property set for this type.
    """

    @property
    def access_request_settings(self):
        """
        AccessRequestSettings is an optional property set to retrieve details for pending access requests if present.
        """
        return self.properties.get("accessRequestSettings", AccessRequestSettings())

    @property
    def anonymous_link_expiration_restriction_days(self):
        # type: () -> Optional[int]
        """Tenant's anonymous link expiration restriction in days."""
        return self.properties.get("anonymousLinkExpirationRestrictionDays", None)

    @property
    def domain_restriction_settings(self):
        """Whether DomainRestrictionSettings is used to limit the external Users set by Admin."""
        return self.properties.get(
            "anonymousLinkExpirationRestrictionDays", DomainRestrictionSettings()
        )

    @property
    def permissions_information(self):
        """
        The PermissionCollection that are on the list item. It contains a collection of PrincipalInfo and LinkInfo.
        """
        return self.properties.get("permissionsInformation", PermissionCollection())

    @property
    def picker_settings(self):
        """PickerSettings used by the PeoplePicker Control."""
        return self.properties.get(
            "pickerSettings",
            PickerSettings(
                self.context, ResourcePath("pickerSettings", self.resource_path)
            ),
        )

    @property
    def sharing_abilities(self):
        """
        Matrix of possible sharing abilities per sharing type and the state of each capability for the current user
        on the list item."""
        return self.properties.get("sharingAbilities", SharingAbilities())

    @property
    def sharing_link_templates(self):
        """"""
        return self.properties.get(
            "sharingLinkTemplates", SharingLinkDefaultTemplatesCollection()
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "accessRequestSettings": self.access_request_settings,
                "domain_restriction_settings": self.domain_restriction_settings,
                "permissionsInformation": self.permissions_information,
                "pickerSettings": self.picker_settings,
                "sharingAbilities": self.sharing_abilities,
                "sharingLinkTemplates": self.sharing_link_templates,
            }
            default_value = property_mapping.get(name, None)
        return super(SharingInformation, self).get_property(name, default_value)

    @property
    def entity_type_name(self):
        return "SP.Sharing.SharingInformation"
