from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.permissions.roles.assignments.assignment import RoleAssignment


class RoleAssignmentCollection(EntityCollection[RoleAssignment]):
    """Represents a collection of RoleAssignment resources."""

    def __init__(self, context, resource_path=None):
        super(RoleAssignmentCollection, self).__init__(
            context, RoleAssignment, resource_path
        )

    def __getitem__(self, key):
        """
        :param int or str key: key is used to address a RoleAssignment resource by either an index
        in collection or by resource id"""
        if isinstance(key, int):
            return super(RoleAssignmentCollection, self).__getitem__(key)
        else:
            return self._item_type(self.context, ResourcePath(key, self.resource_path))

    def get_by_principal_id(self, principal_id):
        """Retrieves the role assignment object (1) based on the specified user or group.

        :param int principal_id: Specifies the user or group of the role assignment.
        """
        return RoleAssignment(
            self.context,
            ServiceOperationPath(
                "GetByPrincipalId", [principal_id], self.resource_path
            ),
        )

    def add_role_assignment(self, principal_id, role_def_id):
        """Adds a role assignment to the role assignment collection.

        :param int principal_id: Specifies the user or group of the role assignment.
        :param int role_def_id: Specifies the role definition of the role assignment.
        """
        payload = {"principalId": principal_id, "roleDefId": role_def_id}
        qry = ServiceOperationQuery(
            self, "AddRoleAssignment", payload, None, None, None
        )
        self.context.add_query(qry)
        return self

    def remove_role_assignment(self, principal_id, role_def_id):
        """Removes the role assignment with the specified principal and role definition from the collection.

        :param int role_def_id: The ID of the role definition in the role assignment.
        :param int principal_id: The ID of the user or group in the role assignment.
        """
        payload = {"principalId": principal_id, "roleDefId": role_def_id}
        qry = ServiceOperationQuery(
            self, "RemoveRoleAssignment", payload, None, None, None
        )
        self.context.add_query(qry)
        return self
