from office365.runtime.client_value import ClientValue


class CurrencyColumn(ClientValue):
    """The currencyColumn on a columnDefinition resource indicates that the column's values represent currency."""

    def __init__(self, locale=None):
        """
        :param str locale: Specifies the locale from which to infer the currency symbol.
        """
        self.locale = locale
