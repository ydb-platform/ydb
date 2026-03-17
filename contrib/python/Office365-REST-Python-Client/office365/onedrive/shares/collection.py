from office365.entity_collection import EntityCollection
from office365.onedrive.internal.paths.shared import SharedPath
from office365.onedrive.shares.drive_item import SharedDriveItem


class SharesCollection(EntityCollection[SharedDriveItem]):
    def __init__(self, context, resource_path=None):
        super(SharesCollection, self).__init__(context, SharedDriveItem, resource_path)

    def by_url(self, url):
        # type: (str) -> SharedDriveItem
        """Address shared item by absolute url"""
        return SharedDriveItem(self.context, SharedPath(url, self.resource_path))
