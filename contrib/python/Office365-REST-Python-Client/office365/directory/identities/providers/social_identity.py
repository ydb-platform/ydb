from typing import Optional

from office365.directory.identities.providers.base import IdentityProviderBase


class SocialIdentityProvider(IdentityProviderBase):
    """
    Represents social identity providers with External Identities for both Azure Active Directory (Azure AD)
    tenant and an Azure AD B2C tenant.
    """

    @property
    def client_id(self):
        # type: () -> Optional[str]
        """
        The client identifier for the application obtained when registering the application with the identity provider.
        """
        return self.properties.get("clientId", None)

    @property
    def client_secret(self):
        # type: () -> Optional[str]
        """
        The client secret for the application that is obtained when the application is registered
        with the identity provider. This is write-only. A read operation returns ****.
        """
        return self.properties.get("clientSecret", None)

    @property
    def identity_provider_type(self):
        # type: () -> Optional[str]
        """
        For a B2B scenario, possible values: Google, Facebook.
        For a B2C scenario, possible values: Microsoft, Google, Amazon, LinkedIn, Facebook, GitHub, Twitter, Weibo,
        QQ, WeChat.
        """
        return self.properties.get("identityProviderType", None)
