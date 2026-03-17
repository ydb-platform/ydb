from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class TranslationNotificationRecipient(ClientValue):
    def __init__(self, login_name=None):
        self.LoginName = login_name


class TranslationNotificationRecipientCollection(ClientValue):
    def __init__(self, language_code=None, recipients=None):
        """
        :param str language_code:
        :param list[str] recipients:
        """
        self.LanguageCode = language_code
        self.Recipients = ClientValueCollection(
            TranslationNotificationRecipient, recipients
        )
