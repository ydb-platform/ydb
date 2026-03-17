from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.planner.tasks.task import PlannerTask
from office365.runtime.paths.resource_path import ResourcePath


class PlannerBucket(Entity):
    """Represents a bucket (or "custom column") for tasks in a plan in Microsoft 365.
    It is contained in a plannerPlan and can have a collection of plannerTasks.
    """

    @property
    def tasks(self):
        """Read-only. Nullable. Collection of tasks in the bucket."""
        return self.properties.get(
            "tasks",
            EntityCollection(
                self.context, PlannerTask, ResourcePath("tasks", self.resource_path)
            ),
        )
