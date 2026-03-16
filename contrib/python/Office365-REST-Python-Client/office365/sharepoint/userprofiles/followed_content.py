from typing import Optional

from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.userprofiles.follow_result import FollowResult


class FollowedContent(Entity):
    """The FollowedContent class provides access to followed content items."""

    def follow_item(self, item):
        """
        The FollowItem method is reserved for server-to-server use only.
        The server sets the specified item to be followed by the current user. This method cannot be called
        from the client.

        :param office365.sharepoint.userprofiles.followed_item.FollowedItem item: Identifies the item to follow.
        """
        return_type = ClientResult(self.context, FollowResult())
        payload = {"item": item}
        qry = ServiceOperationQuery(
            self, "FollowItem", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_followed_status(self, url):
        """
        The GetFollowedStatus method retrieves the followed status of the specified document or site.
        An item can be followed if the url parameter identifies a document or site that the current user has access to.

        :param str url: URL that identifies the item
        """
        return_type = ClientResult(self.context, int())
        payload = {"url": url}
        qry = ServiceOperationQuery(
            self, "GetFollowedStatus", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def followed_documents_url(self):
        # type: () -> Optional[str]
        """The FollowedDocumentsUrl property gets the location of the followed documents view."""
        return self.properties.get("FollowedDocumentsUrl", None)

    @property
    def followed_sites_url(self):
        # type: () -> Optional[str]
        """The FollowedSitesUrl property gets the location of the followed sites view."""
        return self.properties.get("FollowedSitesUrl", None)

    @property
    def entity_type_name(self):
        return "SP.UserProfiles.FollowedContent"
