from office365.runtime.client_value import ClientValue
from office365.sharepoint.search.endpoints import SearchEndpoints
from office365.sharepoint.search.query.context import QueryContext
from office365.sharepoint.search.query.expanded_parameters import (
    ExpandedQueryParameters,
)
from office365.sharepoint.search.query.routing_info import QueryRoutingInfo


class QueryConfiguration(ClientValue):
    """This object contains the query configuration for the local farm and is the response
    to the REST call get query configuration (section 3.1.5.18.2.1.6)."""

    def __init__(
        self,
        query_context=QueryContext(),
        query_parameters=ExpandedQueryParameters(),
        query_routing_info=QueryRoutingInfo(),
        search_endpoints=SearchEndpoints(),
    ):
        """
        :param QueryContext query_context: This property contains the query context.
        :param ExpandedQueryParameters query_parameters: This property contains the expanded query parameters.
        :param QueryRoutingInfo query_routing_info: This property contains the query routing info.
        :param SearchEndpoints search_endpoints: This property contains the search endpoints.
        """
        self.QueryContext = query_context
        self.QueryParameters = query_parameters
        self.QueryRoutingInfo = query_routing_info
        self.SearchEndpoints = search_endpoints

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.QueryConfiguration"
