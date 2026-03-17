from office365.runtime.client_value import ClientValue


class SharedWithUser(ClientValue):
    """Returns the array of users that have been shared in sharing action for the change log."""

    def __init__(self, email=None, name=None):
        """
        :param str email: Gets the email of the principal.
        :param str name: Gets the name of the principal.
        """
        self.Email = email
        self.Name = name
