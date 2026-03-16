from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.custom_result import CustomResult
from office365.sharepoint.search.refinement_results import RefinementResults
from office365.sharepoint.search.relevant_results import RelevantResults
from office365.sharepoint.search.special_term_results import SpecialTermResults


class QueryResult(ClientValue):
    """
    The QueryResult type is a grouping of result tables, where each contained result table is a ResultTable
    as specified in [MS-QSSWS] section 3.1.4.1.3.6.
    """

    def __init__(
        self,
        query_id=None,
        custom_results=None,
        refinement_results=RefinementResults(),
        relevant_results=RelevantResults(),
        query_rule_id=None,
        special_term_results=SpecialTermResults(),
    ):
        """
        :param str query_id: Specifies the identifier for the search query
        :param list[CustomResults] custom_results: CustomResults is a list that contains zero or more CustomResult
            instances. A CustomResult instance is a ResultTable with ResultType of any kind (except RefinementResults,
            RelevantResults, and SpecialTermResults)
        :param RelevantResults relevant_results: This contains a list of query results, all of which are of the
            type specified in TableType.
        :param str query_rule_id: Specifies the unique identifier of the query rule that produced the result set.
            MUST be {00000000-0000-0000-0000-000000000000} if the result set is not associated to a query rule.
        """
        self.QueryId = query_id
        self.QueryRuleId = query_rule_id
        self.RefinementResults = refinement_results
        self.CustomResults = ClientValueCollection(CustomResult, custom_results)
        self.RelevantResults = relevant_results
        self.SpecialTermResults = special_term_results

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.QueryResult"
