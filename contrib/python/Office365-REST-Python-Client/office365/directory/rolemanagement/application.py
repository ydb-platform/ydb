from office365.directory.identitygovernance.privilegedaccess.unified_role_assignment_schedule_request import (
    UnifiedRoleAssignmentScheduleRequest,
)
from office365.directory.rolemanagement.unifiedrole.assignment import (
    UnifiedRoleAssignment,
)
from office365.directory.rolemanagement.unifiedrole.definition import (
    UnifiedRoleDefinition,
)
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.paths.resource_path import ResourcePath


class RbacApplication(Entity):
    """Role management container for unified role definitions and role assignments for Microsoft 365 role-based
    access control (RBAC) providers. The role assignments support only a single principal and a single scope.
    Currently directory and entitlementManagement are the two RBAC providers supported.
    """

    @property
    def role_assignments(self):
        # type: () -> EntityCollection[UnifiedRoleAssignment]
        """Resource to grant access to users or groups."""
        return self.properties.get(
            "roleAssignments",
            EntityCollection(
                self.context,
                UnifiedRoleAssignment,
                ResourcePath("roleAssignments", self.resource_path),
            ),
        )

    @property
    def role_definitions(self):
        # type: () -> EntityCollection[UnifiedRoleDefinition]
        """Resource representing the roles allowed by RBAC providers and the permissions assigned to the roles."""
        return self.properties.get(
            "roleDefinitions",
            EntityCollection(
                self.context,
                UnifiedRoleDefinition,
                ResourcePath("roleDefinitions", self.resource_path),
            ),
        )

    def role_assignment_schedule_requests(self):
        # type: () -> EntityCollection[UnifiedRoleAssignmentScheduleRequest]
        """Resource representing the roles allowed by RBAC providers and the permissions assigned to the roles."""
        return self.properties.get(
            "roleAssignmentScheduleRequests",
            EntityCollection(
                self.context,
                UnifiedRoleAssignmentScheduleRequest,
                ResourcePath("roleAssignmentScheduleRequests", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "roleAssignments": self.role_assignments,
                "roleDefinitions": self.role_definitions,
            }
            default_value = property_mapping.get(name, None)
        return super(RbacApplication, self).get_property(name, default_value)
