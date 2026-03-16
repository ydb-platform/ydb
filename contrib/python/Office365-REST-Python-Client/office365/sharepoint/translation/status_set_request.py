from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.translation.requested_translation import RequestedTranslation


class TranslationStatusSetRequest(ClientValue):
    def __init__(self, values=None):
        """
        :param list[RequestedTranslation] values:
        """
        self.RequestedTranslations = ClientValueCollection(RequestedTranslation, values)
