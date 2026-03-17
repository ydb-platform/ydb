from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.special_term_result import SpecialTermResult


class SpecialTermResults(ClientValue):
    """The SpecialTermResults table contains best bets that apply to the search query."""

    def __init__(self, results=None):
        self.Results = ClientValueCollection(SpecialTermResult, results)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.Search.REST.SpecialTermResults"
