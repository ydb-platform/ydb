from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.administration.webs.templates.template import (
    SPOTenantWebTemplate,
)


class SPOTenantWebTemplateCollection(Entity):
    @property
    def items(self):
        return self.properties.get("Items", ClientValueCollection(SPOTenantWebTemplate))
