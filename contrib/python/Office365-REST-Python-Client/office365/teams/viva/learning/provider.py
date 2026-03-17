from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.viva.learning.content import LearningContent


class LearningProvider(Entity):
    """Represents an entity that holds the details about a learning provider in Viva learning."""

    @property
    def learning_contents(self):
        """Learning catalog items for the provider."""
        return self.properties.get(
            "learningContents",
            EntityCollection(
                self.context,
                LearningContent,
                ResourcePath("learningContents", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "learningContents": self.learning_contents,
            }
            default_value = property_mapping.get(name, None)
        return super(LearningProvider, self).get_property(name, default_value)
