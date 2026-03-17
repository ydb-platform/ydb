from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.lists.currency_information_collection import (
    CurrencyInformationCollection,
)


class CurrencyList(Entity):
    """List of supported currencies."""

    @staticmethod
    def get_list(context):
        """
        Generates a list of all the currencies allowed in SharePoint currency columns.
        The list contains CurrencyInformation objects with display strings and LCIDs for each currency.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        return_type = ClientResult(context, CurrencyInformationCollection())
        qry = ServiceOperationQuery(
            CurrencyList(context), "GetList", None, None, None, return_type, True
        )
        context.add_query(qry)
        return return_type
