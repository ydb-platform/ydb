from office365.delta_collection import DeltaCollection
from office365.directory.users.user import User
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.create_entity import CreateEntityQuery


class UserCollection(DeltaCollection[User]):
    """User's collection"""

    def __init__(self, context, resource_path=None):
        super(UserCollection, self).__init__(context, User, resource_path)

    def get_by_principal_name(self, name):
        """
        Retrieves User by principal name
        :param str name: User principal name
        """
        return User(self.context, ResourcePath(name, self.resource_path))

    def add(self, user_properties):
        """Create a new user.

        :type user_properties: office365.directory.users.profile.UserProfile
        """
        return_type = User(self.context)
        qry = CreateEntityQuery(self, user_properties, return_type)
        self.context.add_query(qry)
        self.add_child(return_type)
        return return_type
