from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.tenant.administration.hubsites.collection import (
    HubSiteCollection,
)


class SPHubSitesUtility(Entity):
    """You can use the class to register sites as hub sites,
    associate existing sites with hub sites, and obtain or update information about hub sites.
    """

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.SharePoint.Portal.SPHubSitesUtility"
            )
        super(SPHubSitesUtility, self).__init__(context, resource_path)

    def get_hub_sites(self):
        """Gets information about all hub sites that the current user can access."""
        return_type = HubSiteCollection(self.context)
        qry = ServiceOperationQuery(self, "GetHubSites", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type
