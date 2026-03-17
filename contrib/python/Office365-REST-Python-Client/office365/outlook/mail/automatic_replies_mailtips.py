from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.outlook.locale_info import LocaleInfo
from office365.runtime.client_value import ClientValue


class AutomaticRepliesMailTips(ClientValue):
    """MailTips about any automatic replies that have been set up on a mailbox."""

    def __init__(
        self,
        message=None,
        message_language=LocaleInfo(),
        scheduled_end_time=DateTimeTimeZone(),
        scheduled_start_time=DateTimeTimeZone(),
    ):
        """
        :param string message: The automatic reply message.
        :param LocaleInfo message_language: 	The language that the automatic reply message is in.
        :param datetime scheduled_end_time: 	The date and time that automatic replies are set to end.
        :param datetime scheduled_start_time: The date and time that automatic replies are set to begin.
        """
        self.message = message
        self.messageLanguage = message_language
        self.scheduledEndTime = scheduled_end_time
        self.scheduledStartTime = scheduled_start_time
