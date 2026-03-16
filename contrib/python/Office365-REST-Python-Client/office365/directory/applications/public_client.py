from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class PublicClientApplication(ClientValue):
    """
    Specifies settings for non-web app or non-web API (for example, mobile or other public clients such as an
    installed application running on a desktop device).
    """

    def __init__(self, redirect_uris=None):
        """
        :param list[str] redirect_uris: Specifies the URLs where user tokens are sent for sign-in, or the redirect
            URIs where OAuth 2.0 authorization codes and access tokens are sent.
        """
        self.redirectUris = StringCollection(redirect_uris)
