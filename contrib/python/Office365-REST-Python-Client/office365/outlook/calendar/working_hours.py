from office365.outlook.calendar.timezones.base import TimeZoneBase
from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class WorkingHours(ClientValue):
    """
    Represents the days of the week and hours in a specific time zone that the user works.

    Having access to a user's working hours is useful in scenarios that handle activity or resource planning.
    You can get and set the working hours of a user as part of the user's mailbox settings.

    You can choose to set a time zone for your working hours differently from the time zone you have set on your
    Outlook client. This can be useful in cases like when you travel to a different time zone than you usually work in.
    You can set the Outlook client to the destination time zone so that Outlook time values are displayed in local
    time while you are there. When other people request work meetings with you in your usual place of work,
    they can still respect your working hours in the appropriate time zone.
    """

    def __init__(
        self, days_of_week=None, end_time=None, start_time=None, timezone=TimeZoneBase()
    ):
        """
        :param list[str] days_of_week: The days of the week on which the user works.
        :param TimeZoneBase timezone: The time zone to which the working hours apply.
        :param str end_time: The time of the day that the user stops working.
        :param str start_time: The time of the day that the user starts working.
        """
        self.daysOfWeek = StringCollection(days_of_week)
        self.timeZone = timezone
        self.endTime = end_time
        self.startTime = start_time
