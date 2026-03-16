from office365.delta_collection import DeltaCollection
from office365.directory.groups.group import Group
from office365.directory.groups.profile import GroupProfile
from office365.runtime.queries.create_entity import CreateEntityQuery


class GroupCollection(DeltaCollection[Group]):
    """Group's collection"""

    def __init__(self, context, resource_path=None):
        super(GroupCollection, self).__init__(context, Group, resource_path)

    def add(self, group_properties):
        """
        Create a Group resource.
        You can create the following types of groups:
           - Microsoft 365 group (unified group)
           - Security group

        :param GroupProfile group_properties: Group properties
        """
        return_type = Group(self.context)
        self.add_child(return_type)
        qry = CreateEntityQuery(self, group_properties, return_type)
        self.context.add_query(qry)
        return return_type

    def create_m365(self, name, description=None, owner=None):
        """
        Creates a Microsoft 365 group.
        If the owners have not been specified, the calling user is automatically added as the owner of the group.
        :param str name: The display name for the group
        :param str description: An optional description for the group
        :param str owner: The group owner
        """
        params = GroupProfile(name, description, True, False, ["Unified"])
        return self.add(params)

    def create_security(self, name, description=None):
        """
        Creates a Security group
        :param str name: The display name for the group
        :param str description: An optional description for the group
        """
        params = GroupProfile(name, description, False, True, [])
        return self.add(params)

    def create_with_team(self, group_name):
        """
        Provision a new group along with a team.

        Note: After the group is successfully created, which can take up to 15 minutes,
        create a Microsoft Teams team using this method could throw an error since
        the group creation process might not be completed. For that scenario prefer submit the request to server via
        execute_query_retry instead of execute_query when using this method.
        :param str group_name: The display name for the group
        """

        def _after_group_created(return_type):
            # type: (Group) -> None
            return_type.add_team()

        return self.create_m365(group_name).after_execute(_after_group_created)

    def get_by_name(self, name):
        # type: (str) -> Group
        """Retrieves group by displayName"""
        return self.single("displayName eq '{0}'".format(name))
