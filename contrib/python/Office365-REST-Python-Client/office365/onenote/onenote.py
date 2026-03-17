from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onenote.notebooks.collection import NotebookCollection
from office365.onenote.operations.onenote import OnenoteOperation
from office365.onenote.pages.collection import OnenotePageCollection
from office365.onenote.resources.resource import OnenoteResource
from office365.onenote.sectiongroups.section_group import SectionGroup
from office365.onenote.sections.section import OnenoteSection
from office365.runtime.paths.resource_path import ResourcePath


class Onenote(Entity):
    """The entry point for OneNote resources."""

    @property
    def notebooks(self):
        # type: () -> NotebookCollection
        """Retrieve a list of notebook objects."""
        return self.properties.get(
            "notebooks",
            NotebookCollection(
                self.context, ResourcePath("notebooks", self.resource_path)
            ),
        )

    @property
    def operations(self):
        """Retrieve a list of OneNote operations."""
        return self.properties.get(
            "operations",
            EntityCollection(
                self.context,
                OnenoteOperation,
                ResourcePath("operations", self.resource_path),
            ),
        )

    @property
    def pages(self):
        # type: () -> OnenotePageCollection
        """Retrieve a list of page objects."""
        return self.properties.get(
            "pages",
            OnenotePageCollection(
                self.context, ResourcePath("pages", self.resource_path)
            ),
        )

    @property
    def resources(self):
        """Retrieve a list of Resources objects from the specified notebook."""
        return self.properties.get(
            "resources",
            EntityCollection(
                self.context,
                OnenoteResource,
                ResourcePath("resources", self.resource_path),
            ),
        )

    @property
    def sections(self):
        # type: () -> EntityCollection[OnenoteSection]
        """Retrieve a list of onenoteSection objects from the specified notebook."""
        return self.properties.get(
            "sections",
            EntityCollection(
                self.context,
                OnenoteSection,
                ResourcePath("sections", self.resource_path),
            ),
        )

    @property
    def section_groups(self):
        """Retrieve a list of onenoteSection objects from the specified notebook."""
        return self.properties.get(
            "sectionGroups",
            EntityCollection(
                self.context,
                SectionGroup,
                ResourcePath("sectionGroups", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"sectionGroups": self.section_groups}
            default_value = property_mapping.get(name, None)
        return super(Onenote, self).get_property(name, default_value)
