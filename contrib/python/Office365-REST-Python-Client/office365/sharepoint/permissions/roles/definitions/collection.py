from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.permissions.roles.definitions.creation_information import (
    RoleDefinitionCreationInformation,
)
from office365.sharepoint.permissions.roles.definitions.definition import RoleDefinition


class RoleDefinitionCollection(EntityCollection[RoleDefinition]):
    """Represents the collection of role definitions that are available within the site"""

    def __init__(self, context, resource_path=None):
        super(RoleDefinitionCollection, self).__init__(
            context, RoleDefinition, resource_path
        )

    def add(self, base_permissions, name, description=None, order=None):
        """
        Adds a new role definition to the collection, based on the passed parameter.

        :param BasePermissions base_permissions: Specifies the base permissions for the role definition.
        :param str name: Specifies the role definition name.
        :param str description: Specifies the description of the role definition. Its length MUST be equal to or less
            than 512.
        :param int order: Specifies the order position of the object (1) in the site collection Permission Levels page.
        """
        return_type = RoleDefinition(self.context)
        self.add_child(return_type)
        payload = RoleDefinitionCreationInformation(
            base_permissions, name, description, order
        )
        qry = CreateEntityQuery(self, payload, return_type)
        self.context.add_query(qry)
        return return_type

    def recreate_missing_default_role_definitions(self):
        """
        Recreates missing default role definitions. Requires that Microsoft.SharePoint.SPWeb has unique
        role definitions.
        """
        qry = ServiceOperationQuery(self, "RecreateMissingDefaultRoleDefinitions")
        self.context.add_query(qry)
        return self

    def remove_all(self):
        """
        Removes all role definitions.
        """
        qry = ServiceOperationQuery(self, "RemoveAll")
        self.context.add_query(qry)
        return self

    def get_by_name(self, name):
        """Returns the role definition matching the name provided.

        :param str name: Specifies name of role definition.
        """
        return RoleDefinition(
            self.context, ServiceOperationPath("GetByName", [name], self.resource_path)
        )

    def get_by_id(self, _id):
        """
        Retrieves the role definition with the specified Id property from the collection.

        :param str _id: Specifies the unique identifier of the role definition searched. The value of id does not
           correspond to the index of the role definition within the collection, but refers to the value of the
           Id property of the role definition.
        """
        return RoleDefinition(
            self.context, ServiceOperationPath("GetById", [_id], self.resource_path)
        )

    def get_by_type(self, role_type):
        """Returns role definition of the specified type from the collection.

        :param int role_type: Specifies the role type. Role type MUST NOT be None.
        """
        return RoleDefinition(
            self.context,
            ServiceOperationPath("GetByType", [role_type], self.resource_path),
        )
