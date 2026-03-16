from typing import Optional

from office365.azure_env import AzureEnvironment
from office365.runtime.auth.token_response import TokenResponse


class AuthenticationContext(object):
    """Provides authentication context for Microsoft Graph client"""

    def __init__(
        self,
        tenant=None,
        scopes=None,
        token_cache=None,
        environment=AzureEnvironment.Global,
    ):
        """
        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param list[str] or None scopes: Scopes requested to access an API
        :param Any token_cache: Default cache is in memory only,
            Refer https://msal-python.readthedocs.io/en/latest/#msal.SerializableTokenCache
        """
        self._tenant = tenant
        if scopes is None:
            scopes = [
                "{0}/.default".format(AzureEnvironment.get_graph_authority(environment))
            ]
        self._scopes = scopes
        self._token_cache = token_cache
        self._token_callback = None
        self._environment = environment

    def acquire_token(self):
        # type: () -> TokenResponse
        """Acquire access token"""
        if not self._token_callback:
            raise ValueError("Token callback is not set.")
        token_resp = self._token_callback()
        token = TokenResponse.from_json(token_resp)
        return token

    def with_access_token(self, token_callback):
        """"""
        self._token_callback = token_callback
        return self

    def with_certificate(self, client_id, thumbprint, private_key):
        """
        Initializes the confidential client with client certificate

        :param str client_id: The OAuth client id of the calling application.
        :param str thumbprint: Thumbprint
        :param str private_key: Private key
        """
        import msal

        app = msal.ConfidentialClientApplication(
            client_id,
            authority=self.authority_url,
            client_credential={
                "thumbprint": thumbprint,
                "private_key": private_key,
            },
            token_cache=self._token_cache,  # Default cache is in memory only.
            # You can learn how to use SerializableTokenCache from
            # https://msal-python.readthedocs.io/en/latest/#msal.SerializableTokenCache
        )

        def _acquire_token():
            return app.acquire_token_for_client(scopes=self._scopes)

        return self.with_access_token(_acquire_token)

    def with_client_secret(self, client_id, client_secret):
        # type: (str, str) -> "AuthenticationContext"
        """
        Initializes the confidential client with client secret

        :param str client_id: The OAuth client id of the calling application.
        :param str client_secret: Client secret
        """
        import msal

        app = msal.ConfidentialClientApplication(
            client_id,
            authority=self.authority_url,
            client_credential=client_secret,
            token_cache=self._token_cache,
        )

        def _acquire_token():
            return app.acquire_token_for_client(scopes=self._scopes)

        return self.with_access_token(_acquire_token)

    def with_token_interactive(self, client_id, username=None):
        # type: (str, Optional[str]) -> "AuthenticationContext"
        """
        Initializes the client via user credentials
        Note: only works if your app is registered with redirect_uri as http://localhost

        :param str client_id: The OAuth client id of the calling application.
        :param str username: Typically a UPN in the form of an email address.
        """
        import msal

        app = msal.PublicClientApplication(client_id, authority=self.authority_url)

        def _acquire_token():
            # The pattern to acquire a token looks like this.
            result = None

            # Firstly, check the cache to see if this end user has signed in before
            accounts = app.get_accounts(username=username)
            if accounts:
                chosen = accounts[0]  # Assuming the end user chose this one to proceed
                # Now let's try to find a token in cache for this account
                result = app.acquire_token_silent(self._scopes, account=chosen)

            if not result:
                result = app.acquire_token_interactive(
                    self._scopes,
                    login_hint=username,
                )
            return result

        return self.with_access_token(_acquire_token)

    def with_username_and_password(self, client_id, username, password):
        # type: (str, str, str) -> "AuthenticationContext"
        """
        Initializes the client via user credentials

        :param str client_id: The OAuth client id of the calling application.
        :param str username: Typically a UPN in the form of an email address.
        :param str password: The password.
        """
        import msal

        app = msal.PublicClientApplication(
            authority=self.authority_url,
            client_id=client_id,
        )

        def _acquire_token():
            result = None
            accounts = app.get_accounts(username=username)
            if accounts:
                result = app.acquire_token_silent(self._scopes, account=accounts[0])

            if not result:
                result = app.acquire_token_by_username_password(
                    username=username,
                    password=password,
                    scopes=self._scopes,
                )
            return result

        return self.with_access_token(_acquire_token)

    @property
    def authority_url(self):
        return "{0}/{1}".format(
            AzureEnvironment.get_login_authority(self._environment), self._tenant
        )
