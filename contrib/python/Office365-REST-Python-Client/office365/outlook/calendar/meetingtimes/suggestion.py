from office365.outlook.calendar.attendees.availability import AttendeeAvailability
from office365.outlook.calendar.meetingtimes.time_slot import TimeSlot
from office365.outlook.mail.location import Location
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class MeetingTimeSuggestion(ClientValue):
    """
    A meeting suggestion that includes information like meeting time, attendance likelihood, individual availability,
    and available meeting locations.
    """

    def __init__(
        self,
        attendee_availability=None,
        confidence=None,
        locations=None,
        meeting_timeslot=TimeSlot(),
    ):
        # type: (list[AttendeeAvailability], float, list[Location], TimeSlot) -> None
        """
        :param attendee_availability: An array that shows the availability status of each
            attendees for this meeting suggestion.
        :param confidence: A percentage that represents the likelhood of all the attendees attending.
        :param locations: An array that specifies the name and geographic location of each meeting location
             for this meeting suggestion.
        :param meeting_timeslot: A time period suggested for the meeting.
        """
        self.attendeeAvailability = ClientValueCollection(
            AttendeeAvailability, attendee_availability
        )
        self.confidence = confidence
        self.locations = ClientValueCollection(Location, locations)
        self.meetingTimeSlot = meeting_timeslot

    def __repr__(self):
        return repr(self.meetingTimeSlot)
