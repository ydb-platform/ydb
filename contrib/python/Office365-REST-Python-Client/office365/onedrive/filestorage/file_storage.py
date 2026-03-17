from office365.entity import Entity
from office365.onedrive.filestorage.container_collection import (
    FileStorageContainerCollection,
)
from office365.runtime.paths.resource_path import ResourcePath


class FileStorage(Entity):
    """Represents the structure of active and deleted fileStorageContainer objects."""

    @property
    def containers(self):
        """The collection of active fileStorageContainers"""
        return self.properties.get(
            "containers",
            FileStorageContainerCollection(
                self.context,
                ResourcePath("containers", self.resource_path),
            ),
        )
