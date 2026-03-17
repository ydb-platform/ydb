from office365.entity_collection import EntityCollection
from office365.onedrive.base_item import BaseItem
from office365.onedrive.recyclebin.item import RecycleBinItem
from office365.onedrive.recyclebin.settings import RecycleBinSettings
from office365.runtime.paths.resource_path import ResourcePath


class RecycleBin(BaseItem):
    """Represents a container for a collection of recycleBinItem resources in a SharePoint site or a
    SharePoint Embedded fileStorageContainer."""

    @property
    def items(self):
        # type: () -> EntityCollection[RecycleBinItem]
        """List of the recycleBinItems deleted by a user."""
        return self.properties.setdefault(
            "items",
            EntityCollection(
                self.context,
                RecycleBinItem,
                ResourcePath("items", self.resource_path),
                self,
            ),
        )

    @property
    def settings(self):
        """Settings for the recycleBin"""
        return self.properties.get("settings", RecycleBinSettings())
