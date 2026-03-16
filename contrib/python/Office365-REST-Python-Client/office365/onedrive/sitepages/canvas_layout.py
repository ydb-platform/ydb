from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.sitepages.sections.horizontal import HorizontalSection
from office365.onedrive.sitepages.sections.vertical import VerticalSection
from office365.runtime.paths.resource_path import ResourcePath


class CanvasLayout(Entity):
    """Represents the layout of the content in a given SharePoint page."""

    @property
    def horizontal_sections(self):
        # type: () -> EntityCollection[HorizontalSection]
        """Collection of horizontal sections on the SharePoint page.."""
        return self.properties.get(
            "horizontalSections",
            EntityCollection(
                self.context,
                HorizontalSection,
                ResourcePath("horizontalSections", self.resource_path),
            ),
        )

    @property
    def vertical_section(self):
        # type: () -> VerticalSection
        """Vertical section on the SharePoint page."""
        return self.properties.get(
            "verticalSection",
            VerticalSection(
                self.context,
                ResourcePath("verticalSection", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "horizontalSections": self.horizontal_sections,
                "verticalSection": self.vertical_section,
            }
            default_value = property_mapping.get(name, None)
        return super(CanvasLayout, self).get_property(name, default_value)
