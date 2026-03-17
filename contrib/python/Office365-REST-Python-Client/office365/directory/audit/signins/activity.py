from office365.runtime.client_value import ClientValue


class SignInActivity(ClientValue):
    """Provides the last interactive or non-interactive sign-in time for a specific user. Since signInActivity
    describes a property of the user object, Azure AD stores sign in activity for your users for as long as the
    user object exists."""

    def __init__(
        self,
        last_non_interactive_sign_in_datetime=None,
        last_non_interactive_sign_in_request_id=None,
        last_sign_in_datetime=None,
        last_sign_in_request_id=None,
    ):
        """
        :param datetime.datetime last_non_interactive_sign_in_datetime: The last non-interactive sign-in date for a
            specific user. You can use this field to calculate the last time a client attempted to sign into the
            directory on behalf of a user. Because some users may use clients to access tenant resources rather
            than signing into your tenant directly, you can use the non-interactive sign-in date to along with
            lastSignInDateTime to identify inactive users.
        :param str last_non_interactive_sign_in_request_id: Request identifier of the last non-interactive sign-in
            performed by this user.
        :param datetime.datetime last_sign_in_datetime: The last interactive sign-in date and time for a specific user.
            You can use this field to calculate the last time a user attempted to sign into the directory with an
            interactive authentication method. This field can be used to build reports, such as inactive users.
            The timestamp represents date and time information using ISO 8601 format and is always in UTC time.
            For example, midnight UTC on Jan 1, 2014 is: '2014-01-01T00:00:00Z'. Azure AD maintains interactive
            sign-ins going back to April 2020. For more information about using the value of this property,
            see Manage inactive user accounts in Azure AD.
        :param str last_sign_in_request_id: Request identifier of the last interactive sign-in performed by this user.
        """
        self.lastNonInteractiveSignInDateTime = last_non_interactive_sign_in_datetime
        self.lastNonInteractiveSignInRequestId = last_non_interactive_sign_in_request_id
        self.lastSignInDateTime = last_sign_in_datetime
        self.lastSignInRequestId = last_sign_in_request_id
