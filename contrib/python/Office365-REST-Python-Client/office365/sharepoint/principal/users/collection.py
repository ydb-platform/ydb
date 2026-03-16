from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.create_entity import CreateEntityQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.principal.users.user import User


class UserCollection(EntityCollection[User]):
    def __init__(self, context, resource_path=None):
        """Represents a collection of User resources."""
        super(UserCollection, self).__init__(context, User, resource_path)

    def add_user(self, user):
        """
        Creates the user
        :param str or User user: Specifies the user login name or User object.
        """
        return_type = User(self.context)
        self.add_child(return_type)

        def _create_and_add_query(login_name):
            # type: (str) -> None
            return_type.set_property("LoginName", login_name)
            qry = CreateEntityQuery(self, return_type, return_type)
            self.context.add_query(qry)

        if isinstance(user, User):

            def _user_loaded():
                _create_and_add_query(user.login_name)

            user.ensure_property("LoginName", _user_loaded)
        else:
            _create_and_add_query(user)
        return return_type

    def get_by_principal_name(self, value):
        # type: (str) -> User
        """Returns the user with the specified principal name."""
        return self.single("UserPrincipalName eq '{0}'".format(value))

    def get_by_email(self, email):
        """
        Returns the user with the specified e-mail address.
        :param str email: A string that contains the e-mail address of the user.
        """
        return User(
            self.context,
            ServiceOperationPath("GetByEmail", [email], self.resource_path),
        )

    def get_by_id(self, user_id):
        """
        Returns the user with the specified member identifier.
        :param int user_id: Specifies the member identifier.
        """
        return User(
            self.context, ServiceOperationPath("GetById", [user_id], self.resource_path)
        )

    def get_by_login_name(self, login_name):
        """
        Retrieve User object by login name
        :param str login_name: A string that contains the login name of the user.
        """
        return User(
            self.context,
            ServiceOperationPath("GetByLoginName", [login_name], self.resource_path),
        )

    def remove_by_id(self, user_id):
        """
        Retrieve User object by id
        :param int user_id: Specifies the member identifier.
        """
        qry = ServiceOperationQuery(self, "RemoveById", [user_id])
        self.context.add_query(qry)
        return self

    def remove_by_login_name(self, login_name):
        """
        Remove User object by login name
        :param str login_name: A string that contains the user name.
        """
        qry = ServiceOperationQuery(self, "RemoveByLoginName", [login_name])
        self.context.add_query(qry)
        return self
