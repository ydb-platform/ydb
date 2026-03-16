from office365.entity import Entity
from office365.onedrive.filestorage.file_storage import FileStorage
from office365.runtime.paths.resource_path import ResourcePath


class Storage(Entity):
    """Facilitates the structures of fileStorageContainers."""

    @property
    def file_storage(self):
        """FileStorageContainer"""
        return self.properties.get(
            "fileStorage",
            FileStorage(self.context, ResourcePath("fileStorage", self.resource_path)),
        )
