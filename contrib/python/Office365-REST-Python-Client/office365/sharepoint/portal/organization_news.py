from office365.runtime.client_result import ClientResult
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class OrganizationNewsSiteReference(ClientValue):
    pass


class OrganizationNews(Entity):
    def sites_reference(self):
        return_type = ClientResult(
            self.context, ClientValueCollection(OrganizationNewsSiteReference)
        )
        qry = ServiceOperationQuery(
            self, "SitesReference", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
