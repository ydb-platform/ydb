from typing import Optional

from office365.entity import Entity


class IdentityProvider(Entity):
    """
    Represents an Azure Active Directory (Azure AD) identity provider.
    The identity provider can be Microsoft, Google, Facebook, Amazon, LinkedIn, or Twitter.
    The following Identity Providers are in Preview: Weibo, QQ, WeChat, GitHub and any OpenID Connect
    supported providers.
    """

    @property
    def client_id(self):
        # type: () -> Optional[str]
        """
        The client ID for the application. This is the client ID obtained when registering the application
        with the identity provider."""
        return self.properties.get("clientId", None)

    @property
    def client_secret(self):
        # type: () -> Optional[str]
        """
        The client secret for the application. This is the client secret obtained when registering the application
        with the identity provider. This is write-only. A read operation will return ****
        """
        return self.properties.get("clientSecret", None)
