from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.teams.viva.learning.courses.activity import LearningCourseActivity


class EmployeeExperienceUser(Entity):
    """"""

    @property
    def learning_course_activities(self):
        # type: () -> EntityCollection[LearningCourseActivity]
        """Get a list of the learningCourseActivity objects (assigned or self-initiated) for a user."""
        return self.properties.get(
            "learningCourseActivities",
            EntityCollection(
                self.context,
                LearningCourseActivity,
                ResourcePath("learningCourseActivities", self.resource_path),
            ),
        )
