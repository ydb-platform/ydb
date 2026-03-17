from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.endpoints import SearchEndpoints


class QueryRoutingInfo(ClientValue):
    """This property contains the query routing info."""

    def __init__(self, query_state=None, search_endpoints=None):
        self.QueryState = query_state
        self.SearchEndpoints = ClientValueCollection(SearchEndpoints, search_endpoints)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.QueryRoutingInfo"
