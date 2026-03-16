from office365.sharepoint.fields.lookup_value import FieldLookupValue
from office365.sharepoint.principal.users.user import User


class FieldUserValue(FieldLookupValue):
    def __init__(self, user_id):
        """Represents the value of a user fields for a list item."""
        super(FieldUserValue, self).__init__(user_id)

    @staticmethod
    def from_user(user):
        # type: (User) -> "FieldUserValue"
        """
        Initialize field value from User
        :param User user: User object
        """
        return_type = FieldUserValue(-1)

        def _user_loaded():
            return_type.LookupId = user.id
            return_type.LookupValue = user.login_name

        user.ensure_properties(["Id", "LoginName"], _user_loaded)
        return return_type
