from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.cdn_url import TenantCdnUrl


class TenantCdnApi(Entity):
    def __init__(self, context):
        super(TenantCdnApi, self).__init__(
            context, ResourcePath("Microsoft.SharePoint.TenantCdn.TenantCdnApi")
        )

    def get_cdn_urls(self, items=None):
        """
        :param list[str] items:
        """
        payload = {
            "items": items,
        }
        return_type = ClientResult(self.context, ClientValueCollection(TenantCdnUrl))
        qry = ServiceOperationQuery(
            self, "GetCdnUrls", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.TenantCdn.TenantCdnApi"
