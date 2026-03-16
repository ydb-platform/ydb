from office365.outlook.calendar.timezones.base import TimeZoneBase


class CustomTimeZone(TimeZoneBase):
    """
    Represents a time zone where the transition from standard to daylight saving time, or vice versa is not standard.
    """

    def __init__(self, bias=None):
        """
        :param int bias: The time offset of the time zone from Coordinated Universal Time (UTC). This value is in
            minutes. Time zones that are ahead of UTC have a positive offset; time zones that are behind UTC have
            a negative offset.
        """
        super(CustomTimeZone, self).__init__()
        self.bias = bias
