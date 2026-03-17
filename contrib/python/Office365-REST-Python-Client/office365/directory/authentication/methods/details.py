from typing import Optional

from office365.entity import Entity


class UserRegistrationDetails(Entity):
    """Represents the state of a user's authentication methods, including which methods are registered and which
    features the user is registered and capable of (such as multi-factor authentication, self-service password reset,
    and passwordless authentication)."""

    @property
    def is_admin(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user has an admin role in the tenant. This value can be used to check the authentication
        methods that privileged accounts are registered for and capable of.
        """
        return self.properties.get("isAdmin", None)

    @property
    def is_mfa_registered(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user has registered a strong authentication method for multi-factor authentication.
        The method may not necessarily be allowed by the authentication methods policy.
        """
        return self.properties.get("isMfaRegistered", None)

    @property
    def is_passwordless_capable(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user has registered a passwordless strong authentication method (including FIDO2,
        Windows Hello for Business, and Microsoft Authenticator (Passwordless)) that is allowed by the authentication
        methods policy.
        """
        return self.properties.get("isPasswordlessCapable", None)

    @property
    def is_sspr_capable(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user has registered the required number of authentication methods for self-service
        password reset and the user is allowed to perform self-service password reset by policy.
        """
        return self.properties.get("isSsprCapable", None)

    @property
    def is_sspr_enabled(self):
        # type: () -> Optional[bool]
        """
        Indicates whether the user is allowed to perform self-service password reset by policy. The user may not
        necessarily have registered the required number of authentication methods for self-service password reset.
        """
        return self.properties.get("isSsprEnabled", None)

    @property
    def user_display_name(self):
        # type: () -> Optional[str]
        """
        The user display name, such as Adele Vance. Supports $filter (eq, startsWith) and $orderby.
        """
        return self.properties.get("userDisplayName", None)

    @property
    def user_preferred_method_for_secondary_authentication(self):
        # type: () -> Optional[str]
        """
        The method the user selected as the default second-factor for performing multifactor authentication.
        """
        return self.properties.get(
            "userPreferredMethodForSecondaryAuthentication", None
        )

    @property
    def user_principal_name(self):
        # type: () -> Optional[str]
        """
        The user principal name, such as AdeleV@contoso.com. Supports $filter (eq, startsWith) and $orderby.
        """
        return self.properties.get("userPrincipalName", None)

    @property
    def user_type(self):
        # type: () -> Optional[str]
        """
        Identifies whether the user is a member or guest in the tenant.
        The possible values are: member, guest, unknownFutureValue.
        """
        return self.properties.get("userType", None)

    def __repr__(self):
        return self.user_principal_name or self.id or self.entity_type_name
