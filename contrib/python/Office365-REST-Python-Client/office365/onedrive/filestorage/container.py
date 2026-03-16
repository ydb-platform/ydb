from datetime import datetime
from typing import Optional

from office365.entity import Entity


class FileStorageContainer(Entity):
    """Represents a location where multiple users or a group of users can store files and access them
    via an application. All file system objects in a fileStorageContainer are returned as driveItem resources.
    """

    @property
    def created_datetime(self):
        # type: () -> Optional[datetime]
        """Date and time of the fileStorageContainer creation. Read-only."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def entity_type_name(self):
        # type: () -> str
        return "graph.fileStorageContainer"

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdDateTime": self.created_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(FileStorageContainer, self).get_property(name, default_value)
