from office365.runtime.client_value import ClientValue
from office365.sharepoint.permissions.base_permissions import BasePermissions


class RoleDefinitionCreationInformation(ClientValue):
    def __init__(
        self,
        base_permissions=BasePermissions(),
        name=None,
        description=None,
        order=None,
    ):
        """Contains properties that are used as parameters to initialize a role definition.

        :param str name: Specifies the name of the role definition.
        :param str description: Specifies the description of the role definition.
        :param int order: Specifies the order in which roles MUST be displayed in the WFE.
        """
        super(RoleDefinitionCreationInformation, self).__init__()
        self.Name = name
        self.Description = description
        self.BasePermissions = base_permissions
        self.Order = order

    @property
    def entity_type_name(self):
        return "SP.RoleDefinition"
