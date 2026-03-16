from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.mail.item_body import ItemBody
from office365.runtime.client_value import ClientValue


class PresenceStatusMessage(ClientValue):
    """Represents a presence status message related to the presence of a user in Microsoft Teams."""

    def __init__(
        self,
        expiry_datetime=DateTimeTimeZone(),
        message=ItemBody(),
        published_datetime=None,
    ):
        """
        :param DateTimeTimeZone expiry_datetime: Time in which the status message expires. If not provided, the status
            message does not expire.
        :param ItemBody message: Status message item.
        :param datetime.datetime published_datetime: Time in which the status message was published.
        """
        self.expiryDateTime = expiry_datetime
        self.message = message
        self.publishedDateTime = published_datetime
