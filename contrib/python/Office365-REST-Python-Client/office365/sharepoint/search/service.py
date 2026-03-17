from typing import Any, List

from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.search.query.auto_completion_results import (
    QueryAutoCompletionResults,
)
from office365.sharepoint.search.query.popular_tenant_query import PopularTenantQuery
from office365.sharepoint.search.query.sort.sort import Sort
from office365.sharepoint.search.query.suggestion_results import QuerySuggestionResults
from office365.sharepoint.search.query.tenant_custom_query_suggestions import (
    TenantCustomQuerySuggestions,
)
from office365.sharepoint.search.request import SearchRequest
from office365.sharepoint.search.result import SearchResult


class SearchService(Entity):
    """SearchService exposes OData Service Operations."""

    def __init__(self, context):
        super(SearchService, self).__init__(
            context, ResourcePath("Microsoft.Office.Server.Search.REST.SearchService")
        )

    def export(self, user, start_time):
        """
        The operation is used by the administrator to retrieve the query log entries,
        issued after a specified date, for a specified user.

        :param datetime.datetime start_time: The timestamp of the oldest query log entry returned.
        :param str or User user: The name of the user or user object that issued the queries.
        """
        return_type = ClientResult(self.context, str())

        def _export(user_name):
            """
            :type user_name: str
            """
            payload = {"userName": user_name, "startTime": start_time.isoformat()}
            qry = ServiceOperationQuery(
                self, "export", None, payload, None, return_type
            )
            self.context.add_query(qry)

        if isinstance(user, User):

            def _user_loaded():
                _export(user.user_principal_name)

            user.ensure_property("UserPrincipalName", _user_loaded)
        else:
            _export(user)
        return return_type

    def export_manual_suggestions(self):
        """ """
        return_type = ClientResult(self.context, TenantCustomQuerySuggestions())
        qry = ServiceOperationQuery(
            self, "exportmanualsuggestions", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def export_popular_tenant_queries(self, count):
        """
        This method is used to get a list of popular search queries executed on the tenant.

        :param int count:
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(PopularTenantQuery)
        )
        payload = {
            "count": count,
        }
        qry = ServiceOperationQuery(
            self, "exportpopulartenantqueries", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def query(
        self,
        query_text,
        source_id=None,
        ranking_model_id=None,
        start_row=None,
        row_limit=None,
        rows_per_page=None,
        select_properties=None,
        refinement_filters=None,
        refiners=None,
        sort_list=None,
        trim_duplicates=None,
        enable_query_rules=None,
        enable_sorting=None,
        **kwargs
    ):
        """The operation is used to retrieve search results by using the HTTP protocol with the GET method.

        :param str query_text: The query text of the search query.
        :param str source_id: Specifies the unique identifier for result source to use for executing the search query.
            If no value is specified then the protocol server MUST use the id for the default result source.
        :param str ranking_model_id: The GUID of the ranking model that SHOULD be used for this search query. If this
            element is not present or a value is not specified, the protocol server MUST use the default ranking model,
            according to protocol server configuration.
        :param int start_row: A zero-based index of the first search result in the list of all search results the
            protocol server returns. The StartRow value MUST be greater than or equal to zero.
        :param int rows_per_page: The number of result items the protocol client displays per page. If this element is
            set to an integer value less than 1, the value of the RowLimit element MUST be used as the default value.
        :param int row_limit: The number of search results the protocol client wants to receive, starting at the index
            specified in the StartRow element. The RowLimit value MUST be greater than or equal to zero.
        :param list[str] select_properties: Specifies a property bag of key value pairs.
        :param list[str] refinement_filters:  The list of refinement tokens for drilldown into search results
        :param list[str] refiners:  Specifies a list of refiners
         :param list[Sort] sort_list:  Specifies the list of properties with which to sort the search results.
        :param bool trim_duplicates:  Specifies whether duplicates are removed by the protocol server before sorting,
             selecting, and sending the search results.
        :param bool enable_sorting: Specifies whether sorting of results is enabled or not.
            MUST ignore the SortList specified if this value is set to false.
        :param bool enable_query_rules: Specifies whether query rules are included when a search query is executed.
            If the value is true, query rules are applied in the search query. If the value is false, query rules
            MUST NOT be applied in the search query.
        """
        params = {
            "querytext": query_text,
            "sourceId": source_id,
            "rankingModelId": ranking_model_id,
            "startRow": start_row,
            "rowsPerPage": rows_per_page,
            "trimDuplicates": trim_duplicates,
            "rowLimit": row_limit,
            "enableQueryRules": enable_query_rules,
            "enableSorting": enable_sorting,
        }
        if refinement_filters:
            params["refinementFilters"] = str(StringCollection(refinement_filters))
        if sort_list:
            params["sortList"] = str(StringCollection([str(s) for s in sort_list]))
        if select_properties:
            params["selectProperties"] = str(StringCollection(select_properties))
        if refiners:
            params["refiners"] = str(StringCollection(refiners))
        params.update(**kwargs)
        return_type = ClientResult(
            self.context, SearchResult()
        )  # type: ClientResult[SearchResult]
        qry = FunctionQuery(self, "query", params, return_type)
        self.context.add_query(qry)
        return return_type

    def post_query(
        self,
        query_text,
        select_properties=None,
        trim_duplicates=None,
        row_limit=None,
        **kwargs
    ):
        # type: (str, List[str], bool, int, Any) -> ClientResult[SearchResult]
        """The operation is used to retrieve search results through the use of the HTTP protocol
        with method type POST.

        :param str query_text: The query text of the search query.
        :param list[str] select_properties: Specifies a property bag of key value pairs.
        :param bool trim_duplicates:  Specifies whether duplicates are removed by the protocol server before sorting,
             selecting, and sending the search results.
        :param int row_limit: The number of search results the protocol client wants to receive, starting at the index
            specified in the StartRow element. The RowLimit value MUST be greater than or equal to zero.
        """
        return_type = ClientResult(self.context, SearchResult())
        request = SearchRequest(
            query_text=query_text,
            select_properties=select_properties,
            trim_duplicates=trim_duplicates,
            row_limit=row_limit,
            **kwargs
        )
        payload = {"request": request}
        qry = ServiceOperationQuery(self, "postquery", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def record_page_click(self, page_info=None, click_type=None, block_type=None):
        """This operation is used by the protocol client to inform the protocol server that a user clicked a
        query result on a page. When a click happens, the protocol client sends the details about the click
        and the page impression for which the query result was clicked to the protocol server.
        This operation MUST NOT be used if no query logging information is returned for a query.
        Also this operation MUST NOT be used if a user clicks a query result for which query logging
        information was not returned

        :param str page_info: Specifies the information about the clicked page, the page impression.
        :param str click_type: Type of clicks. If a particular query result is clicked then the click type returned
             by the search service for this query result MUST be used. If "more" link is clicked then "ClickMore"
             click type MUST be used.
        :param str block_type: Type of query results in the page impression block
        """
        payload = {
            "pageInfo": page_info,
            "clickType": click_type,
            "blockType": block_type,
        }
        qry = ServiceOperationQuery(self, "RecordPageClick", None, payload)
        self.context.add_query(qry)
        return self

    def search_center_url(self):
        # type: () -> ClientResult[str]
        """The operation is used to get the URI address of the search center by using the HTTP protocol
        with the GET method. The operation returns the URI of the of the search center.
        """
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "searchCenterUrl", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def results_page_address(self):
        """The operation is used to get the URI address of the result page by using the HTTP protocol
        with the GET method. The operation returns the URI of the result page."""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "resultspageaddress", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def suggest(self, query_text):
        """
        :param str query_text: The query text of the search query. If this element is not present or a value
             is not specified, a default value of an empty string MUST be used, and the server MUST return a
             FaultException<ExceptionDetail> message.
        """
        return_type = ClientResult(self.context, QuerySuggestionResults())
        payload = {"querytext": query_text}
        qry = ServiceOperationQuery(self, "suggest", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def auto_completions(
        self, query_text, sources=None, number_of_completions=None, cursor_position=None
    ):
        """
        The operation is used to retrieve auto completion results by using the HTTP protocol with the GET method.

        :param str query_text: The query text of the search query. If this element is not present or a value is not
             specified, a default value of an empty string MUST be used, and the server MUST return
             a FaultException<ExceptionDetail> message.
        :param str sources: Specifies the sources that the protocol server SHOULD use when computing the result.
            If NULL, the protocol server SHOULD use all of the sources for autocompletions. The value SHOULD be a
            comma separated set of sources for autocompletions. The set of available sources the server SHOULD support
            is "Tag", which MAY be compiled from the set of #tags applied to documents. If the sources value is not
            a comma separated set of sources, or any of the source does not match "Tag", the server SHOULD return
            completions from all available sources.
        :param int number_of_completions: Specifies the maximum number query completion results in
            GetQueryCompletionsResponse response message.
        :param int cursor_position: Specifies the cursor position in the query text when this operation is sent
            to the protocol server.
        """
        return_type = ClientResult(self.context, QueryAutoCompletionResults())
        payload = {
            "querytext": query_text,
            "sources": sources,
            "numberOfCompletions": number_of_completions,
            "cursorPosition": cursor_position,
        }
        qry = ServiceOperationQuery(
            self, "autocompletions", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
