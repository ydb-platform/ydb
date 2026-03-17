from office365.runtime.client_value import ClientValue


class Language(ClientValue):
    """Represents a natural language."""

    def __init__(self, display_name=None, language_tag=None, lcid=None):
        """
        :param str display_name: Specifies the name of the language as displayed in the user interface.
        :param str language_tag: Specifies the corresponding culture name for the language.
        :param int lcid: Specifies the language code identifier (LCID) for the language.
        """
        self.DisplayName = display_name
        self.LanguageTag = language_tag
        self.Lcid = lcid

    def __str__(self):
        return self.DisplayName

    def __repr__(self):
        return "{0}: {1}".format(self.DisplayName, self.LanguageTag)
