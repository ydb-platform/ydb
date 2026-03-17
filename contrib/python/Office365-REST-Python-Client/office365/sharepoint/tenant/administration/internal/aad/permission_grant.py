from typing_extensions import Self

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection


class SPO3rdPartyAADPermissionGrant(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.TenantAdministration.Internal.SPO3rdPartyAADPermissionGrant"


class SPO3rdPartyAADPermissionGrantCollection(
    EntityCollection[SPO3rdPartyAADPermissionGrant]
):

    def __init__(self, context, resource_path=None):
        super(SPO3rdPartyAADPermissionGrantCollection, self).__init__(
            context, SPO3rdPartyAADPermissionGrant, resource_path
        )

    def add(self, service_principal_id, resource, scope):
        # type: (str, str, str) -> Self
        payload = {
            "servicePrincipalId": service_principal_id,
            "resource": resource,
            "scope": scope,
        }
        qry = ServiceOperationQuery(self, "Add", None, payload)
        self.context.add_query(qry)
        return self
