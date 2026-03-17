from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.teams.apps.resource_specific_permission import (
    TeamsAppResourceSpecificPermission,
)


class TeamsAppPermissionSet(ClientValue):
    """Set of required/granted permissions that can be associated with a Teams app."""

    def __init__(self, resource_specific_permissions=None):
        self.resourceSpecificPermissions = ClientValueCollection(
            TeamsAppResourceSpecificPermission, resource_specific_permissions
        )
