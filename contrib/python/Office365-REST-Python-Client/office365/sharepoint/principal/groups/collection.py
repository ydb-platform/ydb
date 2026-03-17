from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.principal.groups.creation_information import (
    GroupCreationInformation,
)
from office365.sharepoint.principal.groups.group import Group
from office365.sharepoint.utilities.principal_info import PrincipalInfo


class GroupCollection(EntityCollection[Group]):
    """Represents a collection of Group resources."""

    def __init__(self, context, resource_path=None):
        super(GroupCollection, self).__init__(context, Group, resource_path)

    def expand_to_principals(self, max_count):
        """
        Expands groups to a collection of principals.

        :param int max_count: Specifies the maximum number of principals to be returned.
        """
        return_type = ClientResult(self.context, ClientValueCollection(PrincipalInfo))
        for cur_grp in self:
            return_type = cur_grp.expand_to_principals(max_count)
        return return_type

    def add(self, title, description=None):
        """
        Adds a group to the collection. A reference to the SP.Group that was added is returned.

        :param str title: A string that specifies the name of the cross-site group to be created.
            It MUST NOT be NULL. Its length MUST be equal to or less than 255. It MUST NOT be empty.
        :param str description: A string that contains the description of the cross-site group to be created.
            Its length MUST be equal to or less than 512.
        """
        return_type = Group(self.context)
        self.add_child(return_type)
        params = GroupCreationInformation(title, description)
        qry = CreateEntityQuery(self, params, return_type)
        self.context.add_query(qry)
        return return_type

    def get_by_id(self, group_id):
        """Returns the list item with the specified list item identifier.

        :param str group_id: Specifies the member identifier.
        """
        return Group(
            self.context,
            ServiceOperationPath("GetById", [group_id], self.resource_path),
        )

    def get_by_name(self, group_name):
        """Returns a cross-site group from the collection based on the name of the group.

        :param str group_name: A string that contains the name of the group.
        """
        return Group(
            self.context,
            ServiceOperationPath("GetByName", [group_name], self.resource_path),
        )

    def remove_by_id(self, group_id):
        """Removes the group with the specified member ID from the collection.

        :param int group_id: Specifies the member identifier.
        """
        qry = ServiceOperationQuery(self, "RemoveById", [group_id])
        self.context.add_query(qry)
        return self

    def remove_by_login_name(self, group_name):
        """Removes the cross-site group with the specified name from the collection.

        :param str group_name:  A string that contains the name of the group.
        """
        qry = ServiceOperationQuery(self, "RemoveByLoginName", [group_name])
        self.context.add_query(qry)
        return self
