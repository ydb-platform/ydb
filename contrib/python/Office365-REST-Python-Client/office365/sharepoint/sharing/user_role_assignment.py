from office365.runtime.client_value import ClientValue


class UserRoleAssignment(ClientValue):
    """Specifies a user and a role that is associated with the user."""

    def __init__(self, role=None, user_id=None):
        """
        :param int role: Specifies a Role (section 3.2.5.188) that is assigned to a user.
        :param str user_id: Specifies the identifier of a user, which can be in the format of an email address or a
            login identifier.
        """
        self.Role = role
        self.UserId = user_id

    @property
    def entity_type_name(self):
        return "SP.Sharing.UserRoleAssignment"
