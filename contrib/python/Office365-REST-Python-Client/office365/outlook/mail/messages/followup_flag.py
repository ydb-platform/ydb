from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.runtime.client_value import ClientValue


class FollowupFlag(ClientValue):
    """Allows setting a flag in an item for the user to follow up on later."""

    def __init__(
        self,
        completed_datetime=DateTimeTimeZone(),
        due_datetime=DateTimeTimeZone(),
        flag_status=None,
        start_datetime=DateTimeTimeZone(),
    ):
        """
        :param DateTimeTimeZone completed_datetime: The date and time that the follow-up was finished.
        :param DateTimeTimeZone due_datetime: The date and time that the follow up is to be finished.
             Note: To set the due date, you must also specify the startDateTime; otherwise, you will
             get a 400 Bad Request response.
        :param str flag_status: The status for follow-up for an item.
        :param DateTimeTimeZone start_datetime: The date and time that the follow-up is to begin.
        """
        self.completedDateTime = completed_datetime
        self.dueDateTime = due_datetime
        self.flagStatus = flag_status
        self.startDateTime = start_datetime
