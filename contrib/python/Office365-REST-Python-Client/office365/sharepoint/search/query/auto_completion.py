from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.search.query.auto_completion_match import (
    QueryAutoCompletionMatch,
)


class QueryAutoCompletion(ClientValue):
    def __init__(self, query=None, score=None, source=None):
        """
        The QueryAutoCompletion complex type represents the matches for the Query in one Source.

        :param str query: This element represents the query text for the matched results.
        :param float score: This element represents the score for the Query in the Source over all matches in the
             Source.
        :param str source: This element represents the Source used when retrieving the matched results.
        """
        self.Matches = ClientValueCollection(QueryAutoCompletionMatch)
        self.Query = query
        self.Score = score
        self.Source = source

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.QueryAutoCompletion"
