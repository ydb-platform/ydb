from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection
from office365.search.hits_container import SearchHitsContainer


class SearchResponse(ClientValue):
    """Represents results from a search query, and the terms used for the query."""

    def __init__(self, search_terms=None, hits_containers=None):
        """
        :param list[str] search_terms: Contains the search terms sent in the initial search query.
        :param list[SearchHitsContainer] hits_containers: A collection of search results.

        """
        super(SearchResponse, self).__init__()
        self.searchTerms = StringCollection(search_terms)
        self.hitsContainers = ClientValueCollection(
            SearchHitsContainer, hits_containers
        )
