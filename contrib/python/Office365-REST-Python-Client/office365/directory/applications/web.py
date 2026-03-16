from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class WebApplication(ClientValue):
    """Specifies settings for a web application."""

    def __init__(self, home_page_url=None, logout_url=None, redirect_uris=None):
        """
        :param str home_page_url: Home page or landing page of the application.
        :param str logout_url: Specifies the URL that will be used by Microsoft's authorization service to logout an
            user using front-channel, back-channel or SAML logout protocols.
        :param list[str] redirect_uris: Specifies the URLs where user tokens are sent for sign-in, or the redirect
            URIs where OAuth 2.0 authorization codes and access tokens are sent.
        """
        self.homePageUrl = home_page_url
        self.logoutUrl = logout_url
        self.redirectUris = StringCollection(redirect_uris)
