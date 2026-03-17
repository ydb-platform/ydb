from office365.directory.rolemanagement.resource_action import ResourceAction
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class RolePermission(ClientValue):
    """Contains the set of ResourceActions determining the allowed and not allowed permissions for each role."""

    def __init__(self, resource_actions=None):
        self.resourceActions = ClientValueCollection(ResourceAction, resource_actions)
