from office365.outlook.calendar.meetingtimes.time_slot import TimeSlot
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class TimeConstraint(ClientValue):
    """Restricts meeting time suggestions to certain hours and days of the week according to the specified nature of
    activity and open time slots."""

    def __init__(self, activity_domain=None, time_slots=None):
        """
        :param str activity_domain: The nature of the activity, optional
        :param list[TimeSlot] time_slots: An array of time periods
        """
        self.activityDomain = activity_domain
        self.timeSlots = ClientValueCollection(TimeSlot, time_slots)
