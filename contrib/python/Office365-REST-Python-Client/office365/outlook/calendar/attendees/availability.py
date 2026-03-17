from office365.outlook.calendar.attendees.base import AttendeeBase
from office365.runtime.client_value import ClientValue


class AttendeeAvailability(ClientValue):
    """The availability of an attendees."""

    def __init__(self, attendee=AttendeeBase(), availability=None):
        """
        :param AttendeeBase attendee: The email address and type of attendee - whether it's a person or a resource,
             and whether required or optional if it's a person.
        :param str availability: The availability status of the attendee.
        """
        self.attendee = attendee
        self.availability = availability
