import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from typing_extensions import Required, Self, TypedDict

from office365.azure_env import AzureEnvironment
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.providers.acs_token_provider import ACSTokenProvider
from office365.runtime.auth.providers.saml_token_provider import SamlTokenProvider
from office365.runtime.auth.token_response import TokenResponse
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.compat import get_absolute_url
from office365.runtime.http.request_options import RequestOptions

JSONToken = TypedDict(
    "JSONToken",
    {
        "tokenType": Required[str],
        "accessToken": Required[str],
    },
)


def _get_authorization_header(token):
    # type: (Any) -> str
    return "{token_type} {access_token}".format(
        token_type=token.tokenType, access_token=token.accessToken
    )


class AuthenticationContext(object):
    """Authentication context for SharePoint Online/OneDrive For Business"""

    def __init__(self, url, environment=None, allow_ntlm=False, browser_mode=False):
        """
        :param str url: SharePoint absolute web or site Url
        :param str environment: The Office 365 Cloud Environment endpoint used for authentication
            defaults to 'Azure Global'.
        :param bool allow_ntlm: Flag indicates whether NTLM scheme is enabled. Disabled by default
        :param bool browser_mode: Allow browser authentication
        """
        self.url = url.rstrip("/")
        self._authenticate = None
        self._cached_token = None
        self._environment = environment
        self._allow_ntlm = allow_ntlm
        self._browser_mode = browser_mode
        self._token_expires = datetime.max

    def with_client_certificate(
        self,
        tenant,
        client_id,
        thumbprint,
        cert_path=None,
        private_key=None,
        scopes=None,
        passphrase=None,
    ):
        """Initializes a client to acquire a token via certificate credentials

        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param str client_id: The OAuth client id of the calling application.
        :param str thumbprint: Hex encoded thumbprint of the certificate.
        :param str or None cert_path: Path to A PEM encoded certificate private key.
        :param str or None private_key: A PEM encoded certificate private key.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        :param str passphrase: Passphrase if the private_key is encrypted
        """
        if scopes is None:
            resource = get_absolute_url(self.url)
            scopes = ["{url}/.default".format(url=resource)]
        if cert_path is None and private_key is None:
            raise ValueError(
                "Private key is missing. Use either 'cert_path' or 'private_key' to pass the value"
            )
        elif cert_path is not None:
            with open(cert_path, "r", encoding="utf8") as f:
                private_key = f.read()

        def _acquire_token():
            authority_url = "{0}/{1}".format(
                AzureEnvironment.get_login_authority(self._environment), tenant
            )
            credentials = {
                "thumbprint": thumbprint,
                "private_key": private_key,
                "passphrase": passphrase,
            }
            import msal

            app = msal.ConfidentialClientApplication(
                client_id,
                authority=authority_url,
                client_credential=credentials,
            )
            result = app.acquire_token_for_client(scopes)
            return TokenResponse.from_json(result)

        self.with_access_token(_acquire_token)
        return self

    def with_interactive(self, tenant, client_id, scopes=None):
        """
        Initializes a client to acquire a token interactively i.e. via a local browser.

        Prerequisite: In Azure Portal, configure the Redirect URI of your
        "Mobile and Desktop application" as ``http://localhost``.

        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param str client_id: The OAuth client id of the calling application.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        """
        if scopes is None:
            resource = get_absolute_url(self.url)
            scopes = ["{url}/.default".format(url=resource)]

        def _acquire_token():
            import msal

            app = msal.PublicClientApplication(
                client_id,
                authority="{0}/{1}".format(
                    AzureEnvironment.get_login_authority(self._environment), tenant
                ),
                client_credential=None,
            )
            result = app.acquire_token_interactive(scopes=scopes)
            return TokenResponse.from_json(result)

        self.with_access_token(_acquire_token)
        return self

    def with_device_flow(self, tenant, client_id, scopes=None):
        """
        Obtain token by a device flow object, with customizable polling effect.

        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param str client_id: The OAuth client id of the calling application.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        """
        if scopes is None:
            resource = get_absolute_url(self.url)
            scopes = ["{url}/.default".format(url=resource)]

        def _acquire_token():
            import msal

            app = msal.PublicClientApplication(
                client_id,
                authority="{0}/{1}".format(
                    AzureEnvironment.get_login_authority(self._environment), tenant
                ),
                client_credential=None,
            )

            flow = app.initiate_device_flow(scopes=scopes)
            if "user_code" not in flow:
                raise ValueError(
                    "Failed to create device flow: %s" % json.dumps(flow, indent=4)
                )

            print(flow["message"])
            sys.stdout.flush()

            result = app.acquire_token_by_device_flow(flow)
            return TokenResponse.from_json(result)

        self.with_access_token(_acquire_token)
        return self

    def with_access_token(self, token_func):
        # type: (Callable[[], JSONToken]) -> None
        """
        Initializes a client to acquire a token from a callback

        :param () -> dict token_func: A token callback
        """

        def _authenticate(request):

            request_time = datetime.now(timezone.utc)

            if self._cached_token is None or request_time > self._token_expires:
                self._cached_token = token_func()
                if hasattr(self._cached_token, "expiresIn"):
                    self._token_expires = request_time + timedelta(
                        seconds=self._cached_token.expiresIn
                    )
            request.set_header(
                "Authorization", _get_authorization_header(self._cached_token)
            )

        self._authenticate = _authenticate
        return self

    def with_credentials(self, credentials):
        # type: (UserCredential | ClientCredential) -> "AuthenticationContext"
        """
        Initializes a client to acquire a token via user or client credentials
        """
        if isinstance(credentials, ClientCredential):
            provider = ACSTokenProvider(
                self.url,
                credentials.clientId,
                credentials.clientSecret,
                self._environment,
            )
        elif isinstance(credentials, UserCredential):
            if self._allow_ntlm:
                from office365.runtime.auth.providers.ntlm_provider import NtlmProvider

                provider = NtlmProvider(credentials.userName, credentials.password)
            else:
                provider = SamlTokenProvider(
                    self.url,
                    credentials.userName,
                    credentials.password,
                    self._browser_mode,
                    self._environment,
                )
        else:
            raise ValueError("Unknown credential type")

        def _authenticate(request):
            # type: (RequestOptions) -> None
            provider.authenticate_request(request)

        self._authenticate = _authenticate
        return self

    def acquire_token_for_user(self, username, password):
        # type: (str, str) -> Self
        """
        Initializes a client to acquire a token via user credentials
        Status: deprecated!

        :param str password: The user password
        :param str username: Typically a UPN in the form of an email address
        """
        provider = SamlTokenProvider(self.url, username, password, self._browser_mode)

        def _authenticate(request):
            # type: (RequestOptions) -> None
            provider.authenticate_request(request)

        self._authenticate = _authenticate
        return self

    def acquire_token_for_app(self, client_id, client_secret):
        """
        Initializes a client to acquire a token via client credentials (SharePoint App-Only)

        Status: deprecated!

        :param str client_id: The OAuth client id of the calling application.
        :param str client_secret: Secret string that the application uses to prove its identity when requesting a token
        """
        provider = ACSTokenProvider(self.url, client_id, client_secret)

        def _authenticate(request):
            # type: (RequestOptions) -> None
            provider.authenticate_request(request)

        self._authenticate = _authenticate
        return self

    def authenticate_request(self, request):
        # type: (RequestOptions) -> None
        """Authenticate request"""
        if self._authenticate is None:
            raise ValueError("Authentication credentials are missing or invalid")
        self._authenticate(request)
