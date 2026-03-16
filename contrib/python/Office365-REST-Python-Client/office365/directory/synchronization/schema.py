from office365.directory.synchronization.directory_definition import DirectoryDefinition
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class SynchronizationSchema(Entity):
    """
    Defines what objects will be synchronized and how they will be synchronized. The synchronization schema contains
    most of the setup information for a particular synchronization job. Typically, you will customize some of the
    attribute mappings, or add a scoping filter to synchronize only objects that satisfy a certain condition.

    The following sections describe the high-level components of the synchronization schema.
    """

    @property
    def directories(self):
        # type: () -> EntityCollection[DirectoryDefinition]
        """Contains the collection of directories and all of their objects."""
        return self.properties.get(
            "directories",
            EntityCollection(
                self.context,
                DirectoryDefinition,
                ResourcePath("directories", self.resource_path),
            ),
        )
