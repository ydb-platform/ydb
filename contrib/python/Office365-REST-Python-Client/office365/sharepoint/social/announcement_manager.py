from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.webparts.tile_data import TileData


class SocialAnnouncementManager(Entity):
    """Contains methods related to SharePoint Announcement Tiles"""

    @staticmethod
    def get_current_announcements(context, url):
        """
        Gets the currently active announcements for a given site and returns them as a list of TileData objects.
        Announcement details are stored in Title, Description, BackgroundImageLocation, and LinkLocation properties
        of the TileData.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str url:
        """
        return_type = ClientResult(context, ClientValueCollection(TileData))
        manager = SocialAnnouncementManager(context)
        params = {"url": url}
        qry = ServiceOperationQuery(
            manager, "GetCurrentAnnouncements", params, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Social.SocialAnnouncementManager"
