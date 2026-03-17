from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.administration.hubsites.permission import (
    HubSitePermission,
)


class HubSiteProperties(Entity):
    @property
    def permissions(self):
        return self.properties.get(
            "Permissions", ClientValueCollection(HubSitePermission)
        )

    @property
    def site_id(self):
        # type: () -> Optional[str]
        """Returns the Site identifier"""
        return self.properties.get("SiteId", None)

    @property
    def property_ref_name(self):
        return "SiteId"

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.HubSiteProperties"
