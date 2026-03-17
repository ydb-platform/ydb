from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.runtime.client_value import ClientValue


class TimeSlot(ClientValue):
    """Represents a time slot for a meeting."""

    def __init__(self, start=DateTimeTimeZone(), end=DateTimeTimeZone()):
        """

        :param dateTimeTimeZone start: The date, time, and time zone that a period begins.
        :param dateTimeTimeZone end: The date, time, and time zone that a period ends.
        """
        super(TimeSlot, self).__init__()
        self.start = start
        self.end = end

    def __repr__(self):
        return "({0} - {1})".format(self.start, self.end)
