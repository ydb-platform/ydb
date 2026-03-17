from office365.runtime.client_value import ClientValue


class AnalyticsAction(ClientValue):
    """Represents the action in a Microsoft.SharePoint.Client.Search.Analytics.AnalyticsSignal Object."""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Analytics.AnalyticsAction"
