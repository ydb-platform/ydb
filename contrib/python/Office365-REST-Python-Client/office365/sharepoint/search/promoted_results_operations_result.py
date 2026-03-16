from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.object_owner_result import SearchObjectOwnerResult
from office365.sharepoint.search.promoted_result_query_rule import (
    PromotedResultQueryRule,
)


class PromotedResultsOperationsResult(ClientValue):
    """This object contains properties that describes the result of the REST call get promoted results"""

    def __init__(self, result=None, object_owner=SearchObjectOwnerResult()):
        """
        :param list[PromotedResultQueryRule] result: This property contains the collection of the results of
            the promoted results.
        :param SearchObjectOwnerResult object_owner: This property contains the search object owner of the promoted
            result
        """
        self.Result = ClientValueCollection(PromotedResultQueryRule, result)
        self.SearchObjectOwner = object_owner

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.PromotedResultsOperationsResult"
