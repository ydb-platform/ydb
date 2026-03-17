from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.onedrive.sitepages.webparts.web_part import WebPart
from office365.runtime.paths.resource_path import ResourcePath


class VerticalSection(Entity):
    """Represents the vertical section in a given SharePoint page."""

    @property
    def web_parts(self):
        # type: () -> EntityCollection[WebPart]
        """The set of web parts in this section."""
        return self.properties.get(
            "webParts",
            EntityCollection(
                self.context, WebPart, ResourcePath("webParts", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "webParts": self.web_parts,
            }
            default_value = property_mapping.get(name, None)
        return super(VerticalSection, self).get_property(name, default_value)
