from office365.runtime.client_value import ClientValue


class CurrencyInformation(ClientValue):
    """Information about a currency necessary for currency identification and display in the UI."""

    def __init__(self, display_string=None, language_culture_name=None, lcid=None):
        """
        :param str display_string: The Display String (ex: $123,456.00 (United States)) for a specific currency
            which contains a sample formatted value (the currency and the number formatting from the web's locale)
            and the name of the country/region for the currency.
        :param str language_culture_name:
        :param str lcid: The LCID (locale identifier) for a specific currency.
        """
        self.DisplayString = display_string
        self.LanguageCultureName = language_culture_name
        self.LCID = lcid
