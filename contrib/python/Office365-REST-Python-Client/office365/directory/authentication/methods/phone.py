from office365.directory.authentication.methods.method import AuthenticationMethod
from office365.runtime.queries.service_operation import ServiceOperationQuery


class PhoneAuthenticationMethod(AuthenticationMethod):
    """
    Represents a phone number and type that's registered to a user, and whether it's configured for the user
    to sign in via SMS.

    A phone has one of three types: mobile, alternate mobile, or office. A user can have one number registered
    for each type, and must have a mobile phone before an alternate mobile phone is added. When using a phone for
    multi-factor authentication (MFA) or self-service password reset (SSPR), the mobile phone is the default and the
    alternate mobile phone is the backup.

    Mobile phones can be used for both SMS and voice calls, depending on the tenant settings.

    An office phone can only receive voice calls, not SMS messages.
    """

    def disable_sms_signin(self):
        """
        Disable SMS sign-in for an existing mobile phone number registered to a user.
        The number will no longer be available for SMS sign-in, which can prevent your user from signing in.
        """
        qry = ServiceOperationQuery(self, "disableSmsSignIn")
        self.context.add_query(qry)
        return self

    def enable_sms_signin(self):
        """
        Enable SMS sign-in for an existing mobile phone number registered to a user. To be successfully enabled:

        The phone must have "phoneType": "mobile".
        The phone must be unique in the SMS sign-in system (no one else can also be using that number).
        The user must be enabled for SMS sign-in in the authentication methods policy.
        """
        qry = ServiceOperationQuery(self, "enableSmsSignIn")
        self.context.add_query(qry)
        return self

    @property
    def phone_number(self):
        """
        The phone number to text or call for authentication.
        Phone numbers use the format +{country code} {number}x{extension}, with extension optional.
        For example, +1 5555551234 or +1 5555551234x123 are valid. Numbers are rejected when creating or updating
        if they do not match the required format.
        """
        return self.properties.get("phoneNumber", None)
