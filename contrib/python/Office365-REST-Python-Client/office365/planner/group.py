from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class PlannerGroup(Entity):
    """
    The plannerGroup resource provides access to Planner resources for a group.
    It doesn't contain any usable properties.
    """

    @property
    def plans(self):
        """
        Returns the plannerPlans owned by the group.
        """
        from office365.planner.plans.collection import PlannerPlanCollection

        return self.properties.get(
            "plans",
            PlannerPlanCollection(
                self.context, ResourcePath("plans", self.resource_path)
            ),
        )
