from office365.reports.userregistration.method_count import UserRegistrationMethodCount
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class UserRegistrationMethodSummary(ClientValue):
    """Represents the summary of number of users registered for each authentication method."""

    def __init__(
        self,
        total_user_count=None,
        method_counts=None,
        user_roles=None,
        user_types=None,
    ):
        """
        :param int total_user_count: Total number of users in the tenant.
        :param list[UserRegistrationMethodCount] method_counts: Number of users registered for each authentication
           method.
        :param str user_roles: The role type of the user.
             Possible values are: all, privilegedAdmin, admin, user, unknownFutureValue.
        :param str user_types: User type. Possible values are: all, member, guest, unknownFutureValue.
        """
        self.totalUserCount = total_user_count
        self.userRegistrationMethodCounts = ClientValueCollection(
            UserRegistrationMethodCount, method_counts
        )
        self.userRoles = user_roles
        self.userTypes = user_types
