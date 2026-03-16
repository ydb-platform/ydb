from office365.runtime.client_value import ClientValue


class CallOptions(ClientValue):
    """Represents an abstract base class that contains the optional features for a call."""

    def __init__(
        self,
        hide_bot_after_escalation=None,
        is_content_sharing_notification_enabled=None,
    ):
        """
        :param bool hide_bot_after_escalation: Indicates whether to hide the app after the call is escalated.
        :param bool is_content_sharing_notification_enabled: Indicates whether content sharing notifications should be
           enabled for the call.
        """
        self.hideBotAfterEscalation = hide_bot_after_escalation
        self.isContentSharingNotificationEnabled = (
            is_content_sharing_notification_enabled
        )
