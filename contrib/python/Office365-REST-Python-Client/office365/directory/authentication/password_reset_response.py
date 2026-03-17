from office365.runtime.client_value import ClientValue


class PasswordResetResponse(ClientValue):
    """
    Represents the new system-generated password after a password reset operation.
    """

    def __init__(self, new_password=None):
        """
        :param str new_password: The Azure AD-generated password.
        """
        self.newPassword = new_password
