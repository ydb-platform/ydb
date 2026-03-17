from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.query.auto_completion import QueryAutoCompletion


class QueryAutoCompletionResults(ClientValue):
    def __init__(self, core_execution_time_ms=None, correlation_id=None, queries=None):
        """The complex type QueryAutoCompletionResults represent the result of the operation GetQueryCompletions
        as specified in section 3.1.4.25.2.1.

        :param int core_execution_time_ms:  This element represent the time spent in the protocol server retrieving
             the result.
        :param str correlation_id: This element represent the correlation identification of the request.
        :param list[QueryAutoCompletion] queries: This complex type represent the list of QueryAutoCompletion as
            specified in section 3.1.4.25.3.4 for the Query
        """
        self.CoreExecutionTimeMs = core_execution_time_ms
        self.CorrelationId = correlation_id
        self.Queries = ClientValueCollection(QueryAutoCompletion, queries)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QueryAutoCompletionResults"
