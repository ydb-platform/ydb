from typing import Optional

from office365.entity_collection import EntityCollection
from office365.onenote.entity_hierarchy_model import OnenoteEntityHierarchyModel
from office365.onenote.notebooks.notebook import Notebook
from office365.onenote.sections.section import OnenoteSection
from office365.runtime.paths.resource_path import ResourcePath


class SectionGroup(OnenoteEntityHierarchyModel):
    """A section group in a OneNote notebook. Section groups can contain sections and section groups."""

    @property
    def section_groups_url(self):
        # type: () -> Optional[str]
        """
        The URL for the sectionGroups navigation property, which returns all the section groups in the section group.
        """
        return self.properties.get("sectionGroupsUrl", None)

    @property
    def sections_url(self):
        # type: () -> Optional[str]
        """The URL for the sections navigation property, which returns all the sections in the section group."""
        return self.properties.get("sectionsUrl", None)

    @property
    def parent_notebook(self):
        # type: () -> Notebook
        """The notebook that contains the section group. Read-only."""
        return self.properties.get(
            "parentNotebook",
            Notebook(self.context, ResourcePath("parentNotebook", self.resource_path)),
        )

    @property
    def sections(self):
        # type: () -> EntityCollection[OnenoteSection]
        """The sections in the section group. Read-only. Nullable."""
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
        # type: () -> EntityCollection[SectionGroup]
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
            property_mapping = {
                "parentNotebook": self.parent_notebook,
                "sectionGroups": self.section_groups,
            }
            default_value = property_mapping.get(name, None)
        return super(SectionGroup, self).get_property(name, default_value)
