from datetime import datetime
from typing import Optional

from office365.teams.schedule.change_request import ScheduleChangeRequest


class OfferShiftRequest(ScheduleChangeRequest):
    """Represents a request to offer a shift to another user in the team."""

    @property
    def recipient_action_datetime(self):
        """
        The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time.
        """
        return self.properties.get("recipientActionDateTime", datetime.min)

    @property
    def recipient_action_message(self):
        # type: () -> Optional[str]
        """
        Custom message sent by recipient of the offer shift request.
        """
        return self.properties.get("recipientActionMessage", None)

    @property
    def recipient_user_id(self):
        # type: () -> Optional[str]
        """
        User ID of the recipient of the offer shift request.
        """
        return self.properties.get("recipientUserId", None)

    @property
    def sender_shift_id(self):
        # type: () -> Optional[str]
        """
        User ID of the sender of the offer shift request.
        """
        return self.properties.get("senderShiftId", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "recipientActionDateTime": self.recipient_action_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(OfferShiftRequest, self).get_property(name, default_value)
