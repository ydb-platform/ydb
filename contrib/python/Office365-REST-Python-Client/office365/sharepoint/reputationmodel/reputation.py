from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class Reputation(Entity):
    """The Reputation static type includes methods to set the reputation properties on a list item."""

    @staticmethod
    def set_rating(context, list_id, item_id, rating, return_type=None):
        """
        The SetRating static method rates an item within the specified list.
        The return value is the average rating for the specified list item.

        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str list_id: A string-represented GUID value specifying the list that the list item belongs to.
        :param int item_id: An integer value that identifies a list item within the list it belongs to.
        :param int rating: An integer value for the rating to be submitted.
            The rating value SHOULD be between 1 and 5; otherwise, the server SHOULD return an exception.
        :param ClientResult return_type: return value
        """
        if return_type is None:
            return_type = ClientResult(context)

        binding_type = Reputation(context)
        payload = {"listID": list_id, "itemID": item_id, "rating": rating}
        qry = ServiceOperationQuery(
            binding_type, "SetRating", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @staticmethod
    def set_like(context, list_id, item_id, like, return_type=None):
        """
        The SetLike static method sets or unsets the like quality for the current user for an item within
           the specified list. The return value is the total number of likes for the specified list item.


        :param office365.sharepoint.client_context.ClientContext context: SharePoint client context
        :param str list_id: A string-represented GUID value specifying the list that the list item belongs to.
        :param int item_id: An integer value that identifies a list item within the list it belongs to.
        :param bool like: A Boolean value that indicates the operation being either like or unlike.
            A True value indicates like.
        :param ClientResult return_type: return value
        """
        if return_type is None:
            return_type = ClientResult(context)
        binding_type = Reputation(context)
        payload = {"listID": list_id, "itemID": item_id, "like": like}
        qry = ServiceOperationQuery(
            binding_type, "SetLike", None, payload, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.ReputationModel.Reputation"
