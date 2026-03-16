from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class MySiteRecommendations(Entity):
    """Provides a method to get site and document recommendations for the current user, and methods to follow or
    stop following a particular item."""

    @staticmethod
    def follow_item(context, uri, personal_site_uri, category):
        """
        The FollowItem method adds the specified document or site to the list of followed content (as described in
        [MS-SOCCSOM] section 3.1.5.38.2.1.1). FollowItem MUST return TRUE if successful or FALSE if not successful.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str uri: URL that identifies the item to follow.
        :param str personal_site_uri: Specifies the location of the personal site of the current user on the farm.
        :param str category: Specifies the type of the item to follow.
        """
        return_type = ClientResult(context)
        payload = {
            "uri": uri,
            "personalSiteUri": personal_site_uri,
            "category": category,
        }
        manager = MySiteRecommendations(context)
        qry = ServiceOperationQuery(
            manager, "FollowItem", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def stop_following_item(context, uri, personal_site_uri, category):
        """
        The StopFollowingItem method removes the specified document or site from list of followed content (as described
        in [MS-SOCCSOM] section 3.1.5.38.2.1.6). StopFollowingItem MUST return TRUE if successful or FALSE if
        not successful.

        :type context: office365.sharepoint.client_context.ClientContext
        :param str uri:  URL that identifies the item to stop following.
        :param str personal_site_uri: Specifies the location of the personal site of the current user on the farm.
        :param str category: Specifies the type of the item to follow.
        """
        return_type = ClientResult(context)
        payload = {
            "uri": uri,
            "personalSiteUri": personal_site_uri,
            "category": category,
        }
        manager = MySiteRecommendations(context)
        qry = ServiceOperationQuery(
            manager, "StopFollowingItem", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.MySiteRecommendations"
