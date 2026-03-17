from typing import Callable, List, Optional

from requests import Response
from typing_extensions import Self

from office365.azure_env import AzureEnvironment
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.token_response import TokenResponse
from office365.runtime.auth.user_credential import UserCredential
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.odata.request import ODataRequest
from office365.runtime.odata.v3.json_light_format import JsonLightFormat


class SharePointRequest(ODataRequest):
    def __init__(
        self,
        base_url,
        environment=AzureEnvironment.Global,
        allow_ntlm=False,
        browser_mode=False,
    ):
        """
        :param str base_url: Absolute Web or Site Url
        :param str environment: The Office 365 Cloud Environment endpoint used for authentication
        :param bool allow_ntlm: Flag indicates whether NTLM scheme is enabled. Disabled by default
        :param bool browser_mode: Allow browser authentication
        """
        super().__init__(JsonLightFormat())
        self._auth_context = AuthenticationContext(
            url=base_url,
            environment=environment,
            allow_ntlm=allow_ntlm,
            browser_mode=browser_mode,
        )
        self.beforeExecute += self._authenticate_request

    def execute_request(self, path):
        # type: (str) -> Response
        request_url = "{0}/{1}".format(self.service_root_url, path)
        return self.execute_request_direct(RequestOptions(request_url))

    def with_credentials(self, credentials):
        # type: (UserCredential|ClientCredential) -> Self
        """
        Initializes a client to acquire a token via user or client credentials
        """
        self._auth_context.with_credentials(credentials)
        return self

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
        # type: (str, str, str, Optional[str], Optional[str], Optional[List[str]], Optional[str]) -> Self
        """
        Creates authenticated SharePoint context via certificate credentials

        :param str tenant: Tenant name
        :param str or None cert_path: Path to A PEM encoded certificate private key.
        :param str or None private_key: A PEM encoded certificate private key.
        :param str thumbprint: Hex encoded thumbprint of the certificate.
        :param str client_id: The OAuth client id of the calling application.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        :param str passphrase: Passphrase if the private_key is encrypted
        """
        self._auth_context.with_client_certificate(
            tenant, client_id, thumbprint, cert_path, private_key, scopes, passphrase
        )
        return self

    def with_device_flow(self, tenant, client_id, scopes=None):
        # type: (str, str, Optional[List[str]]) -> Self
        """
        Initializes a client to acquire a token via device flow auth.

        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param str client_id: The OAuth client id of the calling application.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        """
        self._auth_context.with_device_flow(tenant, client_id, scopes)
        return self

    def with_interactive(self, tenant, client_id, scopes=None):
        # type: (str, str, Optional[List[str]]) -> Self
        """
        Initializes a client to acquire a token interactively i.e. via a local browser.

        Prerequisite: In Azure Portal, configure the Redirect URI of your
        "Mobile and Desktop application" as ``http://localhost``.

        :param str tenant: Tenant name, for example: contoso.onmicrosoft.com
        :param str client_id: The OAuth client id of the calling application.
        :param list[str] or None scopes:  Scopes requested to access a protected API (a resource)
        """
        self._auth_context.with_interactive(tenant, client_id, scopes)
        return self

    def with_access_token(self, token_func):
        # type: (Callable[[], TokenResponse]) -> Self
        """
        Initializes a client to acquire a token from a callback
        :param () -> TokenResponse token_func: A token callback
        """
        self._auth_context.with_access_token(token_func)
        return self

    def _authenticate_request(self, request):
        # type: (RequestOptions) -> None
        """Authenticate request"""
        self._auth_context.authenticate_request(request)

    @property
    def authentication_context(self):
        return self._auth_context

    @property
    def base_url(self):
        """Represents Base Url"""
        return self._auth_context.url

    @property
    def service_root_url(self):
        return "{0}/_api".format(self._auth_context.url)
