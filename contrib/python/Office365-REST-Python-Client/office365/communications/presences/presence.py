from typing import Optional

from office365.communications.presences.status_message import PresenceStatusMessage
from office365.entity import Entity
from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.mail.item_body import ItemBody
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Presence(Entity):
    """Contains information about a user's presence, including their availability and user activity."""

    def clear_presence(self, session_id=None):
        """
        Clear the application's presence session for a user. If it is the user's only presence session,
        the user's presence will change to Offline/Offline.

        :param str session_id: The ID of the application's presence session.
        """
        payload = {"sessionId": session_id}
        qry = ServiceOperationQuery(self, "clearPresence", None, payload)
        self.context.add_query(qry)
        return self

    def clear_user_preferred_presence(self):
        """
        Clear the preferred availability and activity status for a user.
        """
        qry = ServiceOperationQuery(self, "clearUserPreferredPresence")
        self.context.add_query(qry)
        return self

    def set_presence(
        self, session_id, availability=None, activity=None, expiration_duration=None
    ):
        """
        Set the state of a user's presence session as an application.

        :param str session_id: The ID of the application's presence session.
        :param str availability: The base presence information.
        :param str activity: The supplemental information to availability.
        :param str expiration_duration: The expiration of the app presence session. The value is represented in
            ISO 8601 format for durations. If not provided, a default expiration of 5 minutes will be applied.
            The valid duration range is 5-240 minutes (PT5M to PT4H)
        """
        payload = {
            "sessionId": session_id,
            "availability": availability,
            "activity": activity,
            "expirationDuration": expiration_duration,
        }
        qry = ServiceOperationQuery(self, "setPresence", None, payload)
        self.context.add_query(qry)
        return self

    def set_status_message(self, message, expiry=None):
        """
        Set a presence status message for a user. An optional expiration date and time can be supplied.
        :param str or ItemBody message: Status message item.
        :param datetime.datetime expiry: Time in which the status message expires. If not provided, the status message
            doesn't expire.
        """
        if not isinstance(message, ItemBody):
            message = ItemBody(message)
        if expiry:
            expiry = DateTimeTimeZone.parse(expiry)
        payload = {
            "statusMessage": PresenceStatusMessage(
                message=message, expiry_datetime=expiry
            )
        }
        qry = ServiceOperationQuery(self, "setStatusMessage", None, payload)
        self.context.add_query(qry)
        return self

    def set_user_preferred_presence(
        self, availability="Available", activity="Available", expiration_duration=None
    ):
        """
        Set the preferred availability and activity status for a user. If the preferred presence of a user is set,
        the user's presence shows as the preferred status.
        Preferred presence takes effect only when at least one presence session exists for the user. Otherwise,
        the user's presence shows as Offline.
        A presence session is created as a result of a successful setPresence operation, or if the user is signed in
        on a Microsoft Teams client.

        :param str availability: The base presence information.
        :param str activity: The supplemental information to availability.
        :param str expiration_duration: The expiration of the app presence session. The value is represented in
            ISO 8601 format for durations. If not provided, a default expiration of 5 minutes will be applied.
            The valid duration range is 5-240 minutes (PT5M to PT4H)
        """
        payload = {
            "availability": availability,
            "activity": activity,
            "expirationDuration": expiration_duration,
        }
        qry = ServiceOperationQuery(self, "setUserPreferredPresence", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def activity(self):
        # type: () -> Optional[str]
        """
        The supplemental information to a user's availability.
        Possible values are Available, Away, BeRightBack, Busy, DoNotDisturb, InACall, InAConferenceCall, Inactive,
        InAMeeting, Offline, OffWork, OutOfOffice, PresenceUnknown, Presenting, UrgentInterruptionsOnly.
        """
        return self.properties.get("activity", None)

    @property
    def availability(self):
        # type: () -> Optional[str]
        """
        The base presence information for a user.
        Possible values are Available, AvailableIdle, Away, BeRightBack, Busy, BusyIdle, DoNotDisturb, Offline,
           PresenceUnknown
        """
        return self.properties.get("availability", None)
