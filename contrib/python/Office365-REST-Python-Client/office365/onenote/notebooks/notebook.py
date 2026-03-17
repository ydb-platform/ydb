from typing import TYPE_CHECKING

from office365.entity_collection import EntityCollection
from office365.onenote.entity_hierarchy_model import OnenoteEntityHierarchyModel
from office365.onenote.sections.section import OnenoteSection
from office365.runtime.paths.resource_path import ResourcePath


class Notebook(OnenoteEntityHierarchyModel):
    """A OneNote notebook."""

    @property
    def sections(self):
        # type: () -> EntityCollection[OnenoteSection]
        """
        Retrieve a list of onenoteSection objects from the specified notebook.
        """
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
        """
        Retrieve a list of onenoteSection objects from the specified notebook.
        """

        from office365.onenote.sectiongroups.section_group import SectionGroup

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
        return super(Notebook, self).get_property(name, default_value)
