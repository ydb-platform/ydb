from office365.runtime.client_value import ClientValue


class UserRegistrationMethodCount(ClientValue):
    """Represents the number of users registered for an authentication method."""

    def __init__(self, authentication_method=None, user_count=None):
        """
        :param str authentication_method: Name of the authentication method.
        :param str user_count: Number of users registered.
        """
        self.authenticationMethod = authentication_method
        self.userCount = user_count

    def __repr__(self):
        return "{0}: {1}".format(self.authenticationMethod, self.userCount)
