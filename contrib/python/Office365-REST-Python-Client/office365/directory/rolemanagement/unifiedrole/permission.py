from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class UnifiedRolePermission(ClientValue):
    """
    Represents a collection of allowed resource actions and the conditions that must be met for the action to be
    allowed. Resource actions are tasks that can be performed on a resource. For example, an application resource may
    support create, update, delete, and reset password actions.
    """

    def __init__(
        self,
        allowed_resource_actions=None,
        condition=None,
        excluded_resource_actions=None,
    ):
        """
        :param list[str] allowed_resource_actions: Set of tasks that can be performed on a resource. Required.
        :param str condition: Optional constraints that must be met for the permission to be effective.
            Not supported for custom roles.
        :param list[str] excluded_resource_actions: Set of tasks that may not be performed on a resource.
            Not yet supported.
        """
        self.allowedResourceActions = StringCollection(allowed_resource_actions)
        self.condition = condition
        self.excludedResourceActions = StringCollection(excluded_resource_actions)
