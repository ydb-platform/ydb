from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.search.query.property import QueryProperty
from office365.sharepoint.search.query.reordering_rule import ReorderingRule
from office365.sharepoint.search.query.sort.sort import Sort


class SearchRequest(ClientValue):
    """
    The SearchRequest structure defines the HTTP BODY of the HTTP POST postquery operation as specified
    in section 3.1.5.7.2.1.3. The postquery operation together with the SearchRequest structure is similar
    to the query operation as specified in section 3.1.5.7.2.1.4, and is provided as a means to overcome
    Uniform Resource Locator (URL) length limitations that some clients experience with HTTP GET operations.
    """

    def __init__(
        self,
        query_text,
        select_properties=None,
        culture=None,
        trim_duplicates=False,
        row_limit=None,
        rows_per_page=None,
        start_row=None,
        enable_sorting=None,
        sort_list=None,
        query_template=None,
        ranking_model_id=None,
        summary_length=None,
        collapse_specification=None,
        client_type=None,
        enable_query_rules=None,
        source_id=None,
        reordering_rules=None,
        properties=None,
        ui_language=None,
        hit_highlighted_properties=None,
        hit_highlighted_multivalue_property_limit=None,
        **kwargs
    ):
        """
        :param str query_text: The query text of the search query. If this element is not present or a value is not
            specified, a default value of an empty string MUST be used, and the server MUST return a
            FaultException<ExceptionDetail> message.
        :param list[str] or None select_properties: As specified in [MS-QSSWS] section 2.2.4.11.
        :param list[str] or None culture: Specifies the identifier of the language culture of the search query.
            If present, the value MUST be a valid language code identifier (LCID) of a culture name,
            as specified in [RFC3066].
        :param bool or None trim_duplicates:  Specifies whether duplicates are removed by the protocol server
            before sorting, selecting, and sending the search results. A value of "true" indicates that the protocol
            server SHOULD perform duplicate result removal. A value of "false" indicates that the protocol server
            MUST NOT attempt to perform duplicate result removal. If this element is not present or a value is
            not specified, a default value of "true" MUST be used. The algorithm used for duplicate detection is
            specific to the implementation of the protocol server.
        :param str or None ranking_model_id: The GUID of the ranking model that SHOULD be used for this search query.
            If this element is not present or a value is not specified, the protocol server MUST use the default
            ranking model, according to protocol server configuration.
        :param int or None row_limit: The number of search results the protocol client wants to receive, starting at
            the index specified in the StartRow element. The RowLimit value MUST be greater than or equal to zero.
        :param bool or None enable_sorting: Specifies whether sorting of results is enabled or not.
            MUST ignore the SortList specified if this value is set to false.
        :param list[Sort] or None sort_list: Specifies the list of properties with which to sort the search results.
            MUST be a SortCollection data type, as specified in section 3.1.4.7.3.4. If this element is not present
            or a value is not specified, the default managed property Rank and default direction
            of "Descending" MUST be used.
        :param str or None query_template: This is the text that will replace the query text.
            It can contain query variables which a query transform will replace during execution of the query.
        :param int or None summary_length:  The maximum number of characters in the result summary.
            The protocol server MUST return a HitHighlightedSummary property that contains less than or equal
            to SummaryLength number of characters. The SummaryLength value MUST be greater than or equal to zero
            and less than or equal to 10000.
        :param int or None start_row:  zero-based index of the first search result in the list of all search results
            the protocol server returns. The StartRow value MUST be greater than or equal to zero.
        :param int or None rows_per_page: The number of result items the protocol client displays per page.
            If this element is set to an integer value less than 1, the value of the RowLimit element MUST be used
            as the default value.
        :param str or None collapse_specification: A set of collapse specifications containing managed properties
            that are used to determine how to collapse individual search results. Results are collapsed into one or
            a specified number of results if they match any of the individual collapse specifications.
            Within a single collapse specification, results will be collapsed if their properties match
            all of the individual properties in the collapse specification.
        :param str or None client_type: represents the place where the search query is sent from
        :param bool or None enable_query_rules: Specifies whether query rules are included when a search query is
            executed. If the value is true, query rules are applied to the search query. If the value is false,
            query rules MUST NOT be applied in the search query.
        :param str or None source_id: Specifies the unique identifier for result source to use for executing the
            search query. If no value is specified then the protocol server MUST use the id for the default
            result source.
        :param list[ReorderingRule] reordering_rules: Specifies the list of rules used to reorder search results.
        :param list[QueryProperty] properties: Used to transport additional and custom search query data, in a
             name-value structure, from the caller to the server.
        :param int ui_language: Specifies the LCID for UI culture.
        :param list[str] hit_highlighted_properties: A list of properties that the protocol server includes in the
             HitHighlightedProperties for each result
        :param int hit_highlighted_multivalue_property_limit: Specifies the maximum number of hit highlighted values
             of multi-value properties to be returned.
        """
        super(SearchRequest, self).__init__()
        self.Querytext = query_text
        self.SelectProperties = StringCollection(select_properties)
        self.ClientType = client_type
        self.CollapseSpecification = collapse_specification
        self.Culture = culture
        self.EnableSorting = enable_sorting
        self.SortList = ClientValueCollection(Sort, sort_list)
        self.TrimDuplicates = trim_duplicates
        self.RankingModelId = ranking_model_id
        self.RowLimit = row_limit
        self.RowsPerPage = rows_per_page
        self.QueryTemplate = query_template
        self.SummaryLength = summary_length
        self.StartRow = start_row
        self.EnableQueryRules = enable_query_rules
        self.SourceId = source_id
        self.ReorderingRules = ClientValueCollection(ReorderingRule, reordering_rules)
        self.Properties = ClientValueCollection(QueryProperty, properties)
        self.UILanguage = ui_language
        self.HitHighlightedProperties = StringCollection(hit_highlighted_properties)
        self.HitHighlightedMultivaluePropertyLimit = (
            hit_highlighted_multivalue_property_limit
        )
        self.__dict__.update(**kwargs)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SearchRequest"
