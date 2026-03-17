from typing import Optional

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.planner.buckets.bucket import PlannerBucket
from office365.planner.plans.container import PlannerPlanContainer
from office365.planner.plans.details import PlannerPlanDetails
from office365.planner.tasks.task import PlannerTask
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath


class PlannerPlan(Entity):
    """The plannerPlan resource represents a plan in Microsoft 365. A plan can be owned by a group
    and contains a collection of plannerTasks. It can also have a collection of plannerBuckets.
    Each plan object has a details object that can contain more information about the plan.
    For more information about the relationships between groups, plans, and tasks, see Planner.
    """

    def __str__(self):
        return self.title or self.entity_type_name

    def __repr__(self):
        return self.id or self.entity_type_name

    def delete_object(self):
        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.set_header("If-Match", self.properties.get("__etag"))

        return (
            super(PlannerPlan, self).delete_object().before_execute(_construct_request)
        )

    @property
    def container(self):
        """Identity of the user, device, or application which created the plan."""
        return self.properties.get("container", PlannerPlanContainer())

    @property
    def title(self):
        # type: () -> Optional[str]
        """Required. Title of the plan."""
        return self.properties.get("title", None)

    @property
    def created_by(self):
        """Identity of the user, device, or application which created the plan."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def buckets(self):
        # type: () -> EntityCollection[PlannerBucket]
        """Collection of buckets in the plan."""
        return self.properties.get(
            "buckets",
            EntityCollection(
                self.context, PlannerBucket, ResourcePath("buckets", self.resource_path)
            ),
        )

    @property
    def details(self):
        """Additional details about the plan."""
        return self.properties.get(
            "details",
            PlannerPlanDetails(
                self.context, ResourcePath("details", self.resource_path)
            ),
        )

    @property
    def tasks(self):
        # type: () -> EntityCollection[PlannerTask]
        """Collection of tasks in the plan."""
        return self.properties.get(
            "tasks",
            EntityCollection(
                self.context, PlannerTask, ResourcePath("tasks", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
            }
            default_value = property_mapping.get(name, None)
        return super(PlannerPlan, self).get_property(name, default_value)
