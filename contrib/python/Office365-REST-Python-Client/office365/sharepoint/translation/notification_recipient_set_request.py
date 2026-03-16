from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.translation.notification_recipient import (
    TranslationNotificationRecipientCollection,
)


class TranslationNotificationRecipientSetRequest(ClientValue):
    def __init__(self, notification_recipients=None):
        self.NotificationRecipients = ClientValueCollection(
            TranslationNotificationRecipientCollection, notification_recipients
        )
