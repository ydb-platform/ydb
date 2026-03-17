from office365.runtime.client_value import ClientValue


class AnalyticsSignal(ClientValue):
    """Contains data about an action performed by an actor on an item."""

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Analytics.AnalyticsSignal"
