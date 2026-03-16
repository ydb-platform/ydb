from office365.runtime.client_value import ClientValue


class PersonalResultSuggestion(ClientValue):
    def __init__(self, highlighted_title=None, is_best_bet=None, title=None, url=None):
        """
        The PersonalResultSuggestion complex type contains a personal search result suggestion.

        :param str highlighted_title: Title of the suggested result. Tokens that match the corresponding personal
             query MUST be surrounded by the <c0></c0> tags.
        :param bool is_best_bet: MUST be true if the suggested result was a best bet for the query.
        :param str title: Title of the suggested result.
        :param str url: URL of the suggested result.
        """
        self.HighlightedTitle = highlighted_title
        self.IsBestBet = is_best_bet
        self.Title = title
        self.Url = url

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Query.PersonalResultSuggestion"
