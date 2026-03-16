from typing import Optional

from office365.entity import Entity


class IdentityProviderBase(Entity):
    """
    Represents identity providers with External Identities for both Azure Active Directory tenant and
    an Azure AD B2C tenant.
    """

    def __str__(self):
        return self.display_name or self.entity_type_name

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """The display name for the identity provider."""
        return self.properties.get("displayName", None)
