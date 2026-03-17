from office365.outlook.calendar.attendees.base import AttendeeBase
from office365.outlook.calendar.email_address import EmailAddress


class Attendee(AttendeeBase):
    """An event attendees. This can be a person or resource such as a meeting room or equipment,
    that has been set up as a resource on the Exchange server for the tenant."""

    def __init__(
        self,
        email_address=EmailAddress(),
        attendee_type=None,
        proposed_new_time=None,
        status=None,
    ):
        """

        :param office365.mail.emailAddress.EmailAddress emailAddress email_address:
        :param office365.calendar.timeSlot.TimeSlot proposed_new_time:
        :param str status: The attendees's response (none, accepted, declined, etc.) for the event and date-time
            that the response was sent.
        """
        super(Attendee, self).__init__(email_address, attendee_type)
        self.proposedNewTime = proposed_new_time
        self.status = status
