from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.search.query.personal_result_suggestion import (
    PersonalResultSuggestion,
)


class QuerySuggestionQuery(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QuerySuggestionQuery"


class QuerySuggestionResults(ClientValue):
    """
    The QuerySuggestionResults complex type is a container for arrays of query suggestions, people name suggestions,
    and personal result suggestions.
    """

    def __init__(self, people_names=None):
        """
        :param list[str] people_names: People names suggested for the user query. MUST be null if
            ShowPeopleNameSuggestions in properties input element is set to false.
        """
        self.PeopleNames = StringCollection(people_names)
        self.PersonalResults = ClientValueCollection(PersonalResultSuggestion)
        self.PopularResults = ClientValueCollection(PersonalResultSuggestion)
        self.Queries = ClientValueCollection(QuerySuggestionQuery)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QuerySuggestionResults"
