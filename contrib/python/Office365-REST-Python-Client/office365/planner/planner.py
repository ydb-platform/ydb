from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.planner.buckets.bucket import PlannerBucket
from office365.planner.plans.collection import PlannerPlanCollection
from office365.planner.tasks.collection import PlannerTaskCollection
from office365.runtime.paths.resource_path import ResourcePath


class Planner(Entity):
    """
    The planner resource is the entry point for the Planner object model.
    It returns a singleton planner resource. It doesn't contain any usable properties.
    """

    @property
    def buckets(self):
        # type: () -> EntityCollection[PlannerBucket]
        """Returns the plannerBuckets assigned to the user."""
        return self.properties.get(
            "buckets",
            EntityCollection(
                self.context, PlannerBucket, ResourcePath("buckets", self.resource_path)
            ),
        )

    @property
    def tasks(self):
        # type: () -> PlannerTaskCollection
        """Returns the plannerTasks assigned to the user."""
        return self.properties.get(
            "tasks",
            PlannerTaskCollection(
                self.context, ResourcePath("tasks", self.resource_path)
            ),
        )

    @property
    def plans(self):
        # type: () -> PlannerPlanCollection
        """Returns the plannerTasks assigned to the user."""
        return self.properties.get(
            "plans",
            PlannerPlanCollection(
                self.context, ResourcePath("plans", self.resource_path)
            ),
        )
