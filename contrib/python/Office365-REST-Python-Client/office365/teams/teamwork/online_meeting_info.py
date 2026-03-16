from office365.runtime.client_value import ClientValue
from office365.teams.teamwork.user_identity import TeamworkUserIdentity


class TeamworkOnlineMeetingInfo(ClientValue):
    """Represents details about an online meeting in Microsoft Teams."""

    def __init__(
        self,
        calendar_event_id=None,
        join_web_url=None,
        organizer=TeamworkUserIdentity(),
    ):
        """
        :param calendar_event_id: The identifier of the calendar event associated with the meeting.
        :param join_web_url: The URL that users click to join or uniquely identify the meeting.
        :param organizer: The organizer associated with the meeting.
        """
        self.calendarEventId = calendar_event_id
        self.joinWebUrl = join_web_url
        self.organizer = organizer
