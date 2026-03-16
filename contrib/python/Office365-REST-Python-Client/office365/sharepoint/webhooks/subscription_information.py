from datetime import datetime, timedelta

from office365.runtime.client_value import ClientValue


class SubscriptionInformation(ClientValue):
    """Parameter class for webhook Update and Create REST operations."""

    def __init__(self, notification_url, resource, expiration_datetime=None):
        """
        :param str notification_url:  Endpoint that will be called when an event occurs. It MUST NOT be empty.
            Its length MUST be equal to or less than 1024.
        :param str resource: The resource path to watch. It MUST NOT be empty.
        :param str expiration_datetime: Expiration time of the subscription.
            The maximum expiration time allowed is 180 days in the future.
        """
        super(SubscriptionInformation, self).__init__()
        self.notificationUrl = notification_url
        self.resource = resource
        if expiration_datetime is None:
            max_expired = datetime.utcnow() + timedelta(days=179)
            expiration_datetime = max_expired.isoformat()
        self.expirationDateTime = expiration_datetime
        self.clientState = None
        self.resourceData = None

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Webhooks.SubscriptionInformation"
