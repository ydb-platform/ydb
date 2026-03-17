from typing import Optional

from office365.directory.permissions.default_user_role import DefaultUserRolePermissions
from office365.directory.policies.base import PolicyBase


class AuthorizationPolicy(PolicyBase):
    """Represents a policy that can control Azure Active Directory authorization settings.
    It's a singleton that inherits from base policy type, and always exists for the tenant.
    """

    @property
    def allowed_to_sign_up_email_based_subscriptions(self):
        # type: () -> Optional[bool]
        """Indicates whether a user can join the tenant by email validation."""
        return self.properties.get("allowedToSignUpEmailBasedSubscriptions", None)

    @property
    def allowed_to_use_sspr(self):
        # type: () -> Optional[bool]
        """Indicates whether users can use the Self-Service Password Reset feature on the tenant."""
        return self.properties.get("allowedToUseSSPR", None)

    @property
    def allow_email_verified_users_to_join_organization(self):
        # type: () -> Optional[bool]
        """Indicates whether a user can join the tenant by email validation."""
        return self.properties.get("allowEmailVerifiedUsersToJoinOrganization", None)

    @property
    def allow_invites_from(self):
        # type: () -> Optional[str]
        """Indicates who can invite external users to the organization.
        Possible values are: none, adminsAndGuestInviters, adminsGuestInvitersAndAllMembers, everyone.
        everyone is the default setting for all cloud environments except US Government.
        """
        return self.properties.get("allowInvitesFrom", None)

    @property
    def allow_user_consent_for_risky_apps(self):
        # type: () -> Optional[bool]
        """Indicates whether user consent for risky apps is allowed.
        We recommend keeping allowUserConsentForRiskyApps as false. Default value is false.
        """
        return self.properties.get("allowUserConsentForRiskyApps", None)

    @property
    def block_msol_powershell(self):
        # type: () -> Optional[bool]
        """To disable the use of MSOL PowerShell, set this property to true. This also disables user-based access to
        the legacy service endpoint used by MSOL PowerShell. This doesn't affect Microsoft Entra Connect
        or Microsoft Graph.
        """
        return self.properties.get("blockMsolPowerShell", None)

    @property
    def default_user_role_permissions(self):
        # type: () -> Optional[DefaultUserRolePermissions]
        """Specifies certain customizable permissions for default user role."""
        return self.properties.get(
            "defaultUserRolePermissions", DefaultUserRolePermissions()
        )
