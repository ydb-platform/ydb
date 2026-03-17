from office365.runtime.client_value import ClientValue


class TrackedItemUpdatesRequest(ClientValue):
    def __init__(self, timestamp=None, tracked_items_as_json=None):
        """
        :param datetime timestamp:
        """
        self.TimeStamp = timestamp
        self.TrackedItemsAsJson = tracked_items_as_json

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Internal.TrackedItemUpdatesRequest"
