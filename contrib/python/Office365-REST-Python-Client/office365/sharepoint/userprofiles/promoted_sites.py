from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.webparts.tile_data import TileData


class PromotedSites(Entity):
    """
    The PromotedSites object provides access to a collection of site links that are visible to all users.
    """

    @staticmethod
    def add_site_link(context, url, title, description=None, image_url=None):
        """
        Creates a new site link in the collection of promoted sites.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url: Specifies the URL of the promoted site.
        :param str title: Specifies a string with the title of the promoted site.
        :param str description: Specifies the description of the promoted site.
        :param str image_url: Specifies a URL of an image representing the promoted site.
        """
        payload = {
            "url": url,
            "title": title,
            "description": description,
            "imageUrl": image_url,
        }
        binding_type = PromotedSites(context)
        qry = ServiceOperationQuery(
            binding_type, "AddSiteLink", None, payload, is_static=True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def delete_site_link(context, item_id):
        """
        Removes the promoted site with the specified identifier from the collection.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param int item_id: Specifies a number that represents the identifier of the site to remove
        """
        payload = {"itemID": item_id}
        binding_type = PromotedSites(context)
        qry = ServiceOperationQuery(
            binding_type, "DeleteSiteLink", None, payload, is_static=True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def get_promoted_links_as_tiles(context):
        """
        Retrieves the collection of promoted site links.
        """
        return_type = ClientResult(context, ClientValueCollection(TileData))
        binding_type = PromotedSites(context)
        qry = ServiceOperationQuery(
            binding_type,
            "GetPromotedLinksAsTiles",
            return_type=return_type,
            is_static=True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.PromotedSites"
