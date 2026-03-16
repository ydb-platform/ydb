from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class SignalStore(Entity):
    """Provides methods for managing the analytics signal store."""

    def __init__(self, context, resource_path):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.SharePoint.Client.Search.Analytics.SignalStore"
            )
        super(SignalStore, self).__init__(context, resource_path)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Client.Search.Analytics.SignalStore"
