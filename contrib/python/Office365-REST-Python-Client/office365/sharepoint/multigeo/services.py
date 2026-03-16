from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.multigeo.storage_quota import StorageQuota
from office365.sharepoint.multigeo.unified_group import UnifiedGroup
from office365.sharepoint.multigeo.user_personal_site_location import (
    UserPersonalSiteLocation,
)


class MultiGeoServices(Entity):
    """
    Multi-Geo capabilities in OneDrive and SharePoint enable control of shared resources like SharePoint team sites
    and Microsoft 365 Group mailboxes stored at rest in a specified geo location.

    Each user, Group mailbox, and SharePoint site have a Preferred Data Location (PDL) which denotes the geo location
    where related data is to be stored. Users' personal data (Exchange mailbox and OneDrive) along with any
    Microsoft 365 Groups or SharePoint sites that they create can be stored in the specified geo location to meet
    data residency requirements.
    """

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.MultiGeo.Service.MultiGeoServicesBeta"
        )
        super(MultiGeoServices, self).__init__(context, static_path)

    def user_personal_site_location(self, user_principal_name):
        # type: (str) -> UserPersonalSiteLocation
        return_type = UserPersonalSiteLocation(self.context)
        qry = ServiceOperationQuery(
            self,
            "UserPersonalSiteLocation",
            [user_principal_name],
            None,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    @property
    def storage_quotas(self):
        # type: () -> EntityCollection[UnifiedGroup]
        """ """
        return self.properties.get(
            "StorageQuotas",
            EntityCollection(
                self.context,
                StorageQuota,
                ResourcePath("StorageQuotas", self.resource_path),
            ),
        )

    @property
    def unified_groups(self):
        # type: () -> EntityCollection[UnifiedGroup]
        """ """
        return self.properties.get(
            "UnifiedGroups",
            EntityCollection(
                self.context,
                UnifiedGroup,
                ResourcePath("UnifiedGroups", self.resource_path),
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MultiGeo.Service.MultiGeoServicesBeta"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"UnifiedGroups": self.unified_groups}
            default_value = property_mapping.get(name, None)
        return super(MultiGeoServices, self).get_property(name, default_value)
