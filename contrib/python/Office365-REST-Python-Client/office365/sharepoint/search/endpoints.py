from office365.runtime.client_value import ClientValue
from office365.sharepoint.search.query.context import QueryContext


class SearchEndpoints(ClientValue):
    """This property contains the search endpoints."""

    def __init__(self, admin_endpoint=None, query_context=QueryContext()):
        self.AdminEndpoint = admin_endpoint
        self.QueryContext = query_context

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SearchEndpoints"
