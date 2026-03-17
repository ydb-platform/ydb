from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.tenant.administration.modified_property import (
    ModifiedProperty,
)


class AuditData(ClientValue):

    def __init__(self, client_ip=None, correlation_id=None, modified_properties=None):
        self.ClientIP = client_ip
        self.CorrelationId = correlation_id
        self.ModifiedProperties = ClientValueCollection(
            ModifiedProperty, modified_properties
        )

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Administration.TenantAdmin.AuditData"
