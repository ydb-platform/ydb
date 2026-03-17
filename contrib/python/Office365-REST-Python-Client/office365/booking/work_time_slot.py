from office365.runtime.client_value import ClientValue


class BookingWorkTimeSlot(ClientValue):
    """Defines the start and end times for work."""

    def __init__(self, end_time=None, start_time=None):
        """
        :param str end_time: The time of the day when work stops. For example, 17:00:00.0000000.
        :param str start_time: The time of the day when work starts. For example, 08:00:00.0000000.
        """
        self.endTime = end_time
        self.startTime = start_time
