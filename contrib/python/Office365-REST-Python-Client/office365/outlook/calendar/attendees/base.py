from office365.outlook.mail.recipient import Recipient


class AttendeeBase(Recipient):
    """The type of attendees."""

    def __init__(self, email_address=None, attendee_type=None):
        """

        :param office365.mail.emailAddress.EmailAddress email_address: Includes the name and SMTP address of the
            attendees
        :param str attendee_type: The type of attendees. The possible values are: required, optional, resource.
            Currently if the attendees is a person, findMeetingTimes always considers the person is of the Required type.
        """
        super(AttendeeBase, self).__init__(email_address)
        self.type = attendee_type
