from datetime import datetime

from office365.directory.permissions.identity_set import IdentitySet
from office365.entity import Entity
from office365.planner.tasks.task_details import PlannerTaskDetails
from office365.runtime.paths.resource_path import ResourcePath


class PlannerTask(Entity):
    """
    The plannerTask resource represents a Planner task in Microsoft 365.
    A Planner task is contained in a plan and can be assigned to a bucket in a plan.
    Each task object has a details object which can contain more information about the task.
    See overview for more information regarding relationships between group, plan and task.
    """

    @property
    def created_by(self):
        """Identity of the user that created the task."""
        return self.properties.get("createdBy", IdentitySet())

    @property
    def created_datetime(self):
        """
        Date and time at which the task is created.
        """
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def title(self):
        """Required. Title of the task."""
        return self.properties.get("title", None)

    @property
    def details(self):
        """Additional details about the task."""
        return self.properties.get(
            "details",
            PlannerTaskDetails(
                self.context, ResourcePath("details", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "createdBy": self.created_by,
                "createdDateTime": self.created_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(PlannerTask, self).get_property(name, default_value)
