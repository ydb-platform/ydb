from typing import Optional

from office365.directory.rolemanagement.app_scope import AppScope
from office365.directory.rolemanagement.unifiedrole.definition import (
    UnifiedRoleDefinition,
)
from office365.entity import Entity
from office365.runtime.paths.resource_path import ResourcePath


class UnifiedRoleAssignment(Entity):
    """
    A role assignment is used to grant access to resources. It represents a role definition assigned to a principal
    (for example, a user or a role-assignable group) at a particular scope.
    """

    @property
    def app_scope_id(self):
        # type: () -> Optional[str]
        """
        Identifier of the app-specific scope when the assignment scope is app-specific. Either this property or
        directoryScopeId is required. App scopes are scopes that are defined and understood by this application only.
        Use / for tenant-wide app scopes. Use directoryScopeId to limit the scope to particular directory objects,
        for example, administrative units. Supports $filter (eq, in).
        """
        return self.properties.get("appScopeId", None)

    @property
    def condition(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("condition", None)

    @property
    def principal_id(self):
        # type: () -> Optional[str]
        """Identifier of the principal to which the assignment is granted. Supported principals are users,
        role-assignable groups, and service principals. Supports $filter (eq, in)."""
        return self.properties.get("principalId", None)

    @property
    def role_definition_id(self):
        # type: () -> Optional[str]
        """Identifier of the unifiedRoleDefinition the assignment is for. Read-only. Supports $filter (eq, in)."""
        return self.properties.get("roleDefinitionId", None)

    @property
    def directory_scope_id(self):
        # type: () -> Optional[str]
        """Identifier of the directory object representing the scope of the assignment.
        The scope of an assignment determines the set of resources for which the principal has been granted access.
        Directory scopes are shared scopes stored in the directory that are understood by multiple applications,
        unlike app scopes that are defined and understood by a resource application only. Supports $filter (eq, in).
        """
        return self.properties.get("directoryScopeId", None)

    @property
    def role_definition(self):
        """
        The roleDefinition the assignment is for. Supports $expand. roleDefinition.Id will be auto expanded.
        """
        return self.properties.get(
            "roleDefinition",
            UnifiedRoleDefinition(
                self.context, ResourcePath("roleDefinition", self.resource_path)
            ),
        )

    @property
    def app_scope(self):
        """
        Read-only property with details of the app specific scope when the assignment scope is app specific.
        Containment entity. Supports $expand for the entitlement provider only.
        """
        return self.properties.get(
            "appScope",
            AppScope(self.context, ResourcePath("appScope", self.resource_path)),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "appScope": self.app_scope,
                "roleDefinition": self.role_definition,
            }
            default_value = property_mapping.get(name, None)
        return super(UnifiedRoleAssignment, self).get_property(name, default_value)
