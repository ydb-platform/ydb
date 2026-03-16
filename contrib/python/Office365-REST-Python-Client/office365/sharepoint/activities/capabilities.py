from office365.runtime.client_value import ClientValue


class ActivityCapabilities(ClientValue):
    def __init__(
        self,
        client_activities_enabled=None,
        client_activities_notification_enabled=None,
        enabled=None,
    ):
        """
        :param bool client_activities_enabled:
        :param bool client_activities_notification_enabled:
        :param bool enabled:
        """
        self.clientActivitiesEnabled = client_activities_enabled
        self.clientActivitiesNotificationEnabled = (
            client_activities_notification_enabled
        )
        self.enabled = enabled

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Activities.ActivityCapabilities"
