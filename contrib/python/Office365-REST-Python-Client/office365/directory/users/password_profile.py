from office365.runtime.client_value import ClientValue


class PasswordProfile(ClientValue):
    """Contains the password profile associated with a user. The passwordProfile property of the user entity is a
    passwordProfile object."""

    def __init__(
        self,
        password=None,
        force_change_password_next_sign_in=None,
        force_change_password_next_sign_in_with_mfa=None,
    ):
        """
        :param str password: The password for the user. This property is required when a user is created.
             It can be updated, but the user will be required to change the password on the next login.
             The password must satisfy minimum requirements as specified by the user's passwordPolicies property.
             By default, a strong password is required.
        :param bool force_change_password_next_sign_in: true if the user must change her password on the next login;
             otherwise false.
        :param bool force_change_password_next_sign_in_with_mfa: f true, at next sign-in, the user must perform a
             multi-factor authentication (MFA) before being forced to change their password. The behavior is identical
             to forceChangePasswordNextSignIn except that the user is required to first perform a multi-factor
             authentication before password change. After a password change, this property will be automatically
             reset to false. If not set, default is false.
        """
        super(PasswordProfile, self).__init__()
        self.password = password
        self.forceChangePasswordNextSignIn = force_change_password_next_sign_in
        self.forceChangePasswordNextSignInWithMfa = (
            force_change_password_next_sign_in_with_mfa
        )
