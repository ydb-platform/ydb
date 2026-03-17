from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.portal.home.service_context import (
    SharePointHomeServiceContext,
)


class SharePointHomeServiceContextBuilder(Entity):
    def get_context(self):
        return_type = SharePointHomeServiceContext(self.context)
        qry = ServiceOperationQuery(self, "Context", None, None, None, return_type)
        self.context.add_query(qry)
        return None

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SharePointHomeServiceContextBuilder"
