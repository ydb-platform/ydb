from office365.outlook.calendar.dateTimeTimeZone import DateTimeTimeZone
from office365.runtime.client_value import ClientValue


class AutomaticRepliesSetting(ClientValue):
    """
    Configuration settings to automatically notify the sender of an incoming email with a message from the signed-in
    user. For example, an automatic reply to notify that the signed-in user is unavailable to respond to emails.
    """

    def __init__(
        self,
        external_audience=None,
        external_reply_message=None,
        internal_reply_message=None,
        scheduled_end_datetime=DateTimeTimeZone(),
        scheduled_start_datetime=DateTimeTimeZone(),
        status=None,
    ):
        """
        :param str external_audience: The set of audience external to the signed-in user's organization who will
           receive the ExternalReplyMessage, if Status is AlwaysEnabled or Scheduled.
           The possible values are: none, contactsOnly, all.
        :param str external_reply_message: The automatic reply to send to the specified external audience, if Status
           is AlwaysEnabled or Scheduled.
        :param str internal_reply_message: The automatic reply to send to the audience internal
            to the signed-in user's organization, if Status is AlwaysEnabled or Scheduled.
        """
        self.externalAudience = external_audience
        self.externalReplyMessage = external_reply_message
        self.internalReplyMessage = internal_reply_message
        self.scheduledEndDateTime = scheduled_end_datetime
        self.scheduledStartDateTime = scheduled_start_datetime
        self.status = status
