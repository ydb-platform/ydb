from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.viva.community import Community
from office365.teams.viva.learning.courses.activity import LearningCourseActivity
from office365.teams.viva.learning.provider import LearningProvider


class EmployeeExperience(Entity):
    """Represents a container that exposes navigation properties for employee experience resources."""

    @property
    def communities(self):
        """A collection of communities in Viva Engage."""
        return self.properties.get(
            "communities",
            EntityCollection(
                self.context,
                Community,
                ResourcePath("communities", self.resource_path),
            ),
        )

    @property
    def learning_course_activities(self):
        """A collection of learning course activities."""
        return self.properties.get(
            "learningCourseActivities",
            EntityCollection(
                self.context,
                LearningCourseActivity,
                ResourcePath("learningCourseActivities", self.resource_path),
            ),
        )

    @property
    def learning_providers(self):
        """A collection of learning providers."""
        return self.properties.get(
            "learningProviders",
            EntityCollection(
                self.context,
                LearningProvider,
                ResourcePath("learningProviders", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "learningCourseActivities": self.learning_course_activities,
                "learningProviders": self.learning_providers,
            }
            default_value = property_mapping.get(name, None)
        return super(EmployeeExperience, self).get_property(name, default_value)
