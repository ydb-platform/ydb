from office365.runtime.client_object import ClientObject
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.tenant.administration.sites.creation_source import (
    SiteCreationSource,
)


class SiteCollectionManagementService(ClientObject):
    def __init__(self, context):
        path = ResourcePath(
            "Microsoft.Online.SharePoint.TenantAdministration.SiteCollectionManagementService"
        )
        super(SiteCollectionManagementService, self).__init__(context, path)

    def export_csv_file(self, view_xml):
        return_type = ClientResult(self.context, str())
        payload = {"viewXml": view_xml}
        qry = ServiceOperationQuery(
            self, "ExportCSVFile", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_creation_source(self):
        return_type = ClientResult(self.context, SiteCreationSource())
        qry = ServiceOperationQuery(
            self, "GetSiteCreationSource", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
