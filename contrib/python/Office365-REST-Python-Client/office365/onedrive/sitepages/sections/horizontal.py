from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.sitepages.sections.horizontal_column import (
    HorizontalSectionColumn,
)
from office365.runtime.paths.resource_path import ResourcePath


class HorizontalSection(Entity):
    """Represents a horizontal section in a given SharePoint page."""

    @property
    def columns(self):
        # type: () -> EntityCollection[HorizontalSectionColumn]
        """The set of vertical columns in this section."""
        return self.properties.get(
            "columns",
            EntityCollection(
                self.context,
                HorizontalSectionColumn,
                ResourcePath("columns", self.resource_path),
            ),
        )
