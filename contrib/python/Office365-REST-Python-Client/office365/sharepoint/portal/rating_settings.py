from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class RatingSettings(Entity):
    @staticmethod
    def get_list_rating(context, list_id):
        """
        :type context: office365.sharepoint.client_context.ClientContext
        :param str list_id:  The List identifier.
        """
        return_type = ClientResult(context, int())
        payload = {"listID": list_id}
        qry = ServiceOperationQuery(
            RatingSettings(context),
            "GetListRating",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.RatingSettings"
