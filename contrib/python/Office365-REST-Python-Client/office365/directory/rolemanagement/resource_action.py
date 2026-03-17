from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class ResourceAction(ClientValue):
    """Set of allowed and not allowed actions for a resource."""

    def __init__(self, allowed=None, not_allowed=None):
        """
        :param list[str] allowed: Allowed Actions
        :param list[str] not_allowed: Not Allowed Actions.
        """
        self.allowedResourceActions = StringCollection(allowed)
        self.notAllowedResourceActions = StringCollection(not_allowed)
