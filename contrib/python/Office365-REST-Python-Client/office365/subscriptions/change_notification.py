from office365.runtime.client_value import ClientValue
from office365.subscriptions.encrypted_content import ChangeNotificationEncryptedContent
from office365.subscriptions.resource_data import ResourceData


class ChangeNotification(ClientValue):
    """Represents the notification sent to the subscriber."""

    def __init__(
        self,
        change_type=None,
        client_state=None,
        encrypted_content=ChangeNotificationEncryptedContent(),
        resource=None,
        resource_data=ResourceData(),
    ):
        """
        :param str change_type: Indicates the type of change that will raise the change notification.
            The supported values are: created, updated, deleted. Required.
        :param str client_state: Value of the clientState property sent in the subscription request (if any).
            The maximum length is 255 characters. The client can check whether the change notification came from the
            service by comparing the values of the clientState property. The value of the clientState property sent
            with the subscription is compared with the value of the clientState property received with each change
            notification. Optional.
        :param ChangeNotificationEncryptedContent encrypted_content: Encrypted content attached with the change
            notification. Only provided if encryptionCertificate and includeResourceData were defined during the
            subscription request and if the resource supports it. Optional.
        :param str resource: The URI of the resource that emitted the change notification relative to
             https://graph.microsoft.com. Required
        :param ResourceData resource_data: The content of this property depends on the type of resource being
             subscribed to. Optional.
        """
        self.changeType = change_type
        self.clientState = client_state
        self.encryptedContent = encrypted_content
        self.resource = resource
        self.resourceData = resource_data
