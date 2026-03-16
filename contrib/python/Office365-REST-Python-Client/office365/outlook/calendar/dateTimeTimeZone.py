import pytz

from office365.runtime.client_value import ClientValue


class DateTimeTimeZone(ClientValue):
    """Describes the date, time, and time zone of a point in time."""

    def __init__(self, datetime=None, timezone=None):
        """

        :param str timezone: Represents a time zone, for example, "Pacific Standard Time".
        :param str datetime: A single point of time in a combined date and time representation ({date}T{time};
            for example, 2017-08-29T04:00:00.0000000).
        """
        super(DateTimeTimeZone, self).__init__()
        self.dateTime = datetime
        self.timeZone = timezone

    def __repr__(self):
        return "{0}, {1}".format(self.dateTime, self.timeZone)

    @staticmethod
    def parse(dt):
        """
        Parses from datetime
        :type dt: datetime.datetime
        """
        local_dt = dt.replace(tzinfo=pytz.utc)
        return DateTimeTimeZone(
            datetime=local_dt.isoformat(), timezone=local_dt.strftime("%Z")
        )
