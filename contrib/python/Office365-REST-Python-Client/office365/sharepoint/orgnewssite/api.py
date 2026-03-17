from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.orgnewssite.info import OrgNewsSiteInfo


class OrgNewsSiteApi(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.SharePoint.OrgNewsSite.OrgNewsSiteApi"
            )
        super(OrgNewsSiteApi, self).__init__(context, resource_path)

    def details(self):
        return_type = ClientResult(self.context, OrgNewsSiteInfo())
        qry = ServiceOperationQuery(self, "Details", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.OrgNewsSite.OrgNewsSiteApi"
