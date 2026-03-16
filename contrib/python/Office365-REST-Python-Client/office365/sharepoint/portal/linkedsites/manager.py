from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.portal.linkedsites.list_contract import (
    LinkedSitesListContract,
)


class SiteLinkingManager(Entity):
    """"""

    def __init__(self, context, resource_path=None):
        super(SiteLinkingManager, self).__init__(context, resource_path)

    def get_site_links(self):
        """ """
        result = ClientResult(self.context, LinkedSitesListContract())
        qry = ServiceOperationQuery(self, "GetSiteLinks", None, None, None, result)
        self.context.add_query(qry)
        return result

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SiteLinkingManager"
