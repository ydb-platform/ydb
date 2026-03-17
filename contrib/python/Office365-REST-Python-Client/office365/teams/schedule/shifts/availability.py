from office365.outlook.mail.patterned_recurrence import PatternedRecurrence
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.teams.schedule.shifts.time_range import TimeRange


class ShiftAvailability(ClientValue):
    """Availability of the user to be scheduled for a shift and its recurrence pattern."""

    def __init__(
        self, recurrence=PatternedRecurrence(), time_slots=None, time_zone=None
    ):
        """
        :param PatternedRecurrence recurrence: Specifies the pattern for recurrence
        :param list[TimeRange] time_slots: The time slot(s) preferred by the user.
        :param str time_zone: Specifies the time zone for the indicated time.
        """
        self.recurrence = recurrence
        self.timeSlots = ClientValueCollection(TimeRange, time_slots)
        self.timeZone = time_zone
