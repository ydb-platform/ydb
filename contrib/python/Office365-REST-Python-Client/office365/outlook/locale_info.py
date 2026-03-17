from office365.runtime.client_value import ClientValue


class LocaleInfo(ClientValue):
    """Information about the locale, including the preferred language and country/region, of the signed-in user."""

    def __init__(self, display_name=None, locale=None):
        """
        :param str display_name: A name representing the user's locale in natural language,
            for example, "English (United States)".
        :param str locale: A locale representation for the user, which includes the user's preferred language
            and country/region. For example, "en-us". The language component follows 2-letter codes as defined in
            ISO 639-1, and the country component follows 2-letter codes as defined in ISO 3166-1 alpha-2.
        """
        self.displayName = display_name
        self.locale = locale

    def __repr__(self):
        return self.locale
