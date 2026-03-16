from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.odata.type import ODataType
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.search.query_result import QueryResult


class SearchResult(ClientValue):
    """
    The SearchResult structure resembles the ResultTableCollection structure
    (specified in [MS-QSSWS] section 3.1.4.1.3.1). However, the individual result tables that share the same
    QueryId are grouped together in a QueryResult structure (specified in section 3.1.5.2).
    The search result tables that have exactly the same QueryId value as specified by the protocol client are grouped
    in the same QueryResult structure accessed through the PrimaryQueryResult property. All other QueryResult buckets
    are organized in a CSOM array of QueryResults accessed through the SecondaryQueryResults property.
    """

    def __init__(
        self,
        elapsed_time=None,
        primary_query_result=QueryResult(),
        properties=None,
        secondary_query_results=None,
        spelling_suggestion=None,
        triggered_rules=None,
    ):
        """
        :param str elapsed_time:  The time it took to execute the search query, in milliseconds.
            This element MUST contain a non-negative number.
        :param QueryResult primary_query_result: A grouping of result tables, where each contained result table is a
            ResultTable as specified in [MS-QSSWS] section 3.1.4.1.3.6.
        :param dict properties: Specifies a property bag of key value pairs
        :param list[QueryResult] secondary_query_results:
        :param str spelling_suggestion: The spelling suggestion for the search query. The protocol server can suggest
            a different spelling of the search query if there is a good chance that the spelling suggestion will
            increase the quality of the search results.<59> The criteria used to determine the spelling suggestion
            and when to show it are specific to the implementation of the protocol server.
        :param list[str] triggered_rules: This element contains the list of unique identifiers of the query rules
            that were executed for the search query.
        """
        super(SearchResult, self).__init__()
        self.PrimaryQueryResult = primary_query_result
        self.ElapsedTime = elapsed_time
        self.Properties = properties
        self.SecondaryQueryResults = ClientValueCollection(
            QueryResult, secondary_query_results
        )
        self.SpellingSuggestion = spelling_suggestion
        self.TriggeredRules = StringCollection(triggered_rules)

    def set_property(self, k, v, persist_changes=True):
        if k == "Properties":
            v = ODataType.parse_key_value_collection(v)
        super(SearchResult, self).set_property(k, v, persist_changes)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SearchResult"
