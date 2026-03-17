from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.runtime.client_value import ClientValue


class ScheduleItem(ClientValue):
    """
    An item that describes the availability of a user corresponding to an actual event on the user's default calendar.
    This item applies to a resource (room or equipment) as well.
    """

    def __init__(
        self,
        start=DateTimeTimeZone(),
        end=DateTimeTimeZone(),
        location=None,
        is_private=None,
        subject=None,
        status=None,
    ):
        """
        :param DateTimeTimeZone start: The date, time, and time zone that the corresponding event starts.
        :param DateTimeTimeZone end: The date, time, and time zone that the corresponding event ends.
        :param str location: The location where the corresponding event is held or attended from. Optional.
        :param bool is_private: The sensitivity of the corresponding event. True if the event is marked private,
             false otherwise. Optional.
        :param str subject: The corresponding event's subject line. Optional.
        :param str status: The availability status of the user or resource during the corresponding event.
              The possible values are: free, tentative, busy, oof, workingElsewhere, unknown.
        """
        self.start = start
        self.end = end
        self.location = location
        self.isPrivate = is_private
        self.subject = subject
        self.status = status
