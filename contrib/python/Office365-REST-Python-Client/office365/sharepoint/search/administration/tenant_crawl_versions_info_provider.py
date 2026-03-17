from typing import TYPE_CHECKING

from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity

if TYPE_CHECKING:
    from office365.sharepoint.client_context import ClientContext


class TenantCrawlVersionsInfoProvider(Entity):
    """Retrieves information about crawl versions for a tenant in SharePoint"""

    @staticmethod
    def create(context):
        # type: (ClientContext) -> TenantCrawlVersionsInfoProvider
        return_type = TenantCrawlVersionsInfoProvider(context)
        qry = ServiceOperationQuery(
            return_type, "Create", None, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    def disable_crawl_versions(self, site_id):
        """
        :param str site_id:
        """
        return_type = ClientResult(self.context, bool())
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(
            self, "DisableCrawlVersions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def is_crawl_versions_enabled(self, site_id):
        """
        :param str site_id:
        """
        return_type = ClientResult(self.context, bool())
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(
            self, "IsCrawlVersionsEnabled", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def is_crawl_versions_enabled_for_tenant(self):
        """ """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "IsCrawlVersionsEnabledForTenant", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Administration.TenantCrawlVersionsInfoProvider"
