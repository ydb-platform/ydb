from office365.directory.authentication.password_reset_response import (
    PasswordResetResponse,
)
from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery


class AuthenticationMethod(Entity):
    """Represents an authentication method registered to a user. An authentication method is something used by a user
    to authenticate or otherwise prove their identity to the system. Some examples include password,
    phone (usable via SMS or voice call), FIDO2 security keys, and more."""

    def reset_password(self, new_password, require_change_on_next_signin):
        """
        Reset a user's password, represented by a password authentication method object. This can only be done by an
        administrator with appropriate permissions and cannot be performed on a user's own account.

        This flow writes the new password to Azure Active Directory and pushes it to on-premises Active Directory
        if configured using password writeback. The admin can either provide a new password or have the system
        generate one. The user is prompted to change their password on their next sign in.

        This reset is a long-running operation and will return a Location header with a link where the caller
        can periodically check for the status of the reset operation.

        :param str new_password: The new password. Required for tenants with hybrid password scenarios.
            If omitted for a cloud-only password, the system returns a system-generated password. This is a unicode
            string with no other encoding. It is validated against the tenant's banned password system before
            acceptance, and must adhere to the tenant's cloud and/or on-premises password requirements.
        :param bool require_change_on_next_signin: Specifies whether the user must change their password at
            their next sign in.
        """
        return_type = ClientResult(self.context, PasswordResetResponse())
        payload = {
            "newPassword": new_password,
            "requireChangeOnNextSignIn": require_change_on_next_signin,
        }
        qry = ServiceOperationQuery(
            self, "resetPassword", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
