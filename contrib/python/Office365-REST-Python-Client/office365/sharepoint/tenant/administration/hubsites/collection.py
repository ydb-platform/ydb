from office365.runtime.client_result import ClientResult
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.tenant.administration.hubsites.hub_site import HubSite


class HubSiteCollection(EntityCollection[HubSite]):
    """Represents a collection of HubSite resources."""

    def __init__(self, context, resource_path=None):
        super(HubSiteCollection, self).__init__(context, HubSite, resource_path)

    def get_by_id(self, _id):
        """Retrieve Hub site by id
        :type _id: str
        """
        return HubSite(
            self.context, ServiceOperationPath("GetById", [_id], self.resource_path)
        )

    def get_connected_hubs(self, hub_site_id, option):
        """
        :param str hub_site_id:
        :param int option:
        """
        payload = {"hubSiteId": hub_site_id, "option": option}
        return_type = HubSiteCollection(self.context)
        qry = ServiceOperationQuery(
            self, "GetConnectedHubs", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_url_by_hub_site_id(self, hub_site_id):
        """
        :param str hub_site_id:
        """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetSiteUrlByHubSiteId", [hub_site_id], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
