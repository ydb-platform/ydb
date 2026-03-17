from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.mail.location import Location
from office365.runtime.client_value import ClientValue


class Reminder(ClientValue):
    """A reminder for an event in a user calendar."""

    def __init__(
        self,
        change_key=None,
        event_end_time=DateTimeTimeZone(),
        event_id=None,
        event_location=Location(),
        event_start_time=DateTimeTimeZone(),
        event_subject=None,
        event_web_link=None,
        reminder_fire_time=DateTimeTimeZone(),
    ):
        """
        :param str change_key: Identifies the version of the reminder. Every time the reminder is changed, changeKey
            changes as well. This allows Exchange to apply changes to the correct version of the object.
        :param DateTimeTimeZone event_end_time: The date, time and time zone that the event ends.
        :param str event_id: The unique ID of the event. Read only.
        :param Location event_location: The location of the event.
        :param DateTimeTimeZone event_start_time: The date, time, and time zone that the event starts.
        :param str event_subject: The text of the event's subject line.
        :param str event_web_link: The URL to open the event in Outlook on the web.
             The event will open in the browser if you are logged in to your mailbox via Outlook on the web.
             You will be prompted to login if you are not already logged in with the browser.
             This URL cannot be accessed from within an iFrame.
        :param DateTimeTimeZone reminder_fire_time: The date, time, and time zone that the reminder is set to occur.
        """
        super(Reminder, self).__init__()
        self.changeKey = change_key
        self.eventStartTime = event_start_time
        self.eventEndTime = event_end_time
        self.eventId = event_id
        self.eventLocation = event_location
        self.eventSubject = event_subject
        self.eventWebLink = event_web_link
        self.reminderFireTime = reminder_fire_time
