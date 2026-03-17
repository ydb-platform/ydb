from datetime import datetime
from typing import Optional

from office365.directory.object import DirectoryObject


class AppRoleAssignment(DirectoryObject):
    """
    Used to record when a user, group, or service principal is assigned an app role for an app.

    An app role assignment is a relationship between the assigned principal (a user, a group, or a service principal),
    a resource application (the app's service principal) and an app role defined on the resource application.
    """

    def __str__(self):
        return "Principal: {0}, AppRole: {1}".format(
            self.principal_display_name, self.app_role_id
        )

    @property
    def app_role_id(self):
        # type: () -> Optional[str]
        """
        The identifier (id) for the app role which is assigned to the principal.
        This app role must be exposed in the appRoles property on
        the resource application's service principal (resourceId). If the resource application has not declared
        any app roles, a default app role ID of 00000000-0000-0000-0000-000000000000 can be specified to signal
        that the principal is assigned to the resource app without any specific app roles. Required on create.
        """
        return self.properties.get("appRoleId", None)

    @property
    def created_datetime(self):
        # type: () -> datetime
        """The time when the app role assignment was created."""
        return self.properties.get("createdDateTime", datetime.min)

    @property
    def principal_display_name(self):
        # type: () -> Optional[str]
        """The display name of the user, group, or service principal that was granted the app role assignment."""
        return self.properties.get("principalDisplayName", None)

    @property
    def principal_id(self):
        # type: () -> Optional[str]
        """The unique identifier (id) for the user, security group, or service principal being granted the app role.
        Security groups with dynamic memberships are supported. Required on create."""
        return self.properties.get("principalId", None)

    @property
    def principal_type(self):
        # type: () -> Optional[str]
        """The type of the assigned principal. This can either be User, Group, or ServicePrincipal."""
        return self.properties.get("principalType", None)

    @property
    def resource_display_name(self):
        # type: () -> Optional[str]
        """The display name of the resource app's service principal to which the assignment is made."""
        return self.properties.get("resourceDisplayName", None)

    @property
    def resource_id(self):
        # type: () -> Optional[str]
        """The unique identifier (id) for the resource service principal for which the assignment is made.
        Required on create."""
        return self.properties.get("resourceId", None)
