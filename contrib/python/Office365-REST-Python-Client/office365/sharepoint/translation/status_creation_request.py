from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class TranslationStatusCreationRequest(ClientValue):
    def __init__(self, language_codes=None):
        """
        :param list[str] language_codes:
        """
        self.LanguageCodes = StringCollection(language_codes)
