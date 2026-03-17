from typing import Optional

from office365.sharepoint.entity import Entity


class FeatureDefinition(Entity):
    """Contains the base definition of a feature, including its name, ID, scope, and version."""

    def __str__(self):
        return self.display_name or self.entity_type_name

    @property
    def display_name(self):
        # type: () -> Optional[str]
        return self.properties.get("DisplayName", None)
