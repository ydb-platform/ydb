from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.lists.currency_information import CurrencyInformation


class CurrencyInformationCollection(ClientValue):
    """List of supported currencies: contains CurrencyInformation objects."""

    def __init__(self, items=None):
        self.Items = ClientValueCollection(CurrencyInformation, items)
