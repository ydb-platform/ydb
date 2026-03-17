from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class DocumentsSharedWithMe(Entity):
    """Provides methods for working with a list that shares documents with the current user on the user's personal site.
    All methods in this object are static."""

    @staticmethod
    def get_list_data(context, sort_field_name, is_ascending_sort, offset, row_limit):
        """
        Gets the JSON string containing the row data for a list that shares documents with the current user on the
        user's personal site.
        :param office365.sharepoint.client_context.ClientContext context: Client context
        :param str sort_field_name: Specifies the view field on which to sort the data in the Web Part.
        :param bool is_ascending_sort: Specifies whether the data in the Web Part will be sorted in ascending order.
        :param int offset: Specifies the number of results to skip before displaying the data in the Web Part.
        :param int row_limit: Specifies the maximum number of items to be rendered in the Web Part at one time.
        """
        payload = {
            "sortFieldName": sort_field_name,
            "isAscendingSort": is_ascending_sort,
            "offset": offset,
            "rowLimit": row_limit,
        }
        return_type = ClientResult(context, str())
        qry = ServiceOperationQuery(
            DocumentsSharedWithMe(context),
            "GetListData",
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
        return "Microsoft.SharePoint.Portal.UserProfiles.DocumentsSharedWithMe"
