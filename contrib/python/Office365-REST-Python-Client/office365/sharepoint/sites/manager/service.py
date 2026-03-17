from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.sites.manager.types import TopSiteFilesResult


class SiteManagerService(Entity):
    """ """

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.SharePoint.SiteManager.SiteManagerService"
        )
        super(SiteManagerService, self).__init__(context, static_path)

    def top_files(self, max_count=None):
        """ """
        return_type = ClientResult(self.context, TopSiteFilesResult())
        payload = {"maxCount": max_count}
        qry = ServiceOperationQuery(
            self,
            "TopFiles",
            None,
            payload,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.SiteManager.SiteManagerService"
