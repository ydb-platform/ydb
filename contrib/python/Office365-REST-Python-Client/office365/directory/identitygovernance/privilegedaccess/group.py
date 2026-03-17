from office365.directory.identitygovernance.privilegedaccess.approval import Approval
from office365.directory.identitygovernance.privilegedaccess.schedule.group_assignment_instance import (
    PrivilegedAccessGroupAssignmentScheduleInstance,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class PrivilegedAccessGroup(Entity):
    """The entry point for all resources related to Privileged Identity Management (PIM) for groups."""

    @property
    def assignment_approvals(self):
        """ """
        return self.properties.get(
            "assignmentApprovals",
            EntityCollection(
                self.context,
                Approval,
                ResourcePath("assignmentApprovals", self.resource_path),
            ),
        )

    @property
    def assignment_schedule_instances(self):
        """The instances of assignment schedules to activate a just-in-time access."""
        return self.properties.get(
            "assignmentScheduleInstances",
            EntityCollection(
                self.context,
                PrivilegedAccessGroupAssignmentScheduleInstance,
                ResourcePath("assignmentScheduleInstances", self.resource_path),
            ),
        )
