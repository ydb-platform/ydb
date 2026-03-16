from typing import Callable, Optional

from office365.azure_env import AzureEnvironment
from office365.runtime.auth.entra.authentication_context import AuthenticationContext
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.odata.request import ODataRequest
from office365.runtime.odata.v4.json_format import V4JsonFormat


class GraphRequest(ODataRequest):
    def __init__(
        self, version="v1.0", tenant=None, environment=AzureEnvironment.Global
    ):
        # type: (str, str, str) -> None
        super(GraphRequest, self).__init__(V4JsonFormat())
        self._version = version
        self._environment = environment
        self._auth_context = AuthenticationContext(
            environment=environment, tenant=tenant
        )
        self.beforeExecute += self.authenticate_request

    def with_access_token(self, token_callback):
        # type: (Callable[[], dict]) -> "GraphRequest"
        """
        Initializes the confidential client with token callback.
        """
        self._auth_context.with_access_token(token_callback)
        return self

    def with_certificate(self, client_id, thumbprint, private_key):
        # type: (str, str, str) -> "GraphRequest"
        """
        Initializes the confidential client with client certificate

        :param str client_id: The OAuth client id of the calling application.
        :param str thumbprint: Thumbprint
        :param str private_key: Private key
        """
        self._auth_context.with_certificate(client_id, thumbprint, private_key)
        return self

    def with_client_secret(self, client_id, client_secret):
        # type: (str, str) -> "GraphRequest"
        """
        Initializes the confidential client with client secret

        :param str client_id: The OAuth client id of the calling application.
        :param str client_secret: Client secret
        """
        self._auth_context.with_client_secret(client_id, client_secret)
        return self

    def with_token_interactive(self, client_id, username=None):
        # type: (str, Optional[str]) -> "GraphRequest"
        """
        Initializes the client via user credentials
        Note: only works if your app is registered with redirect_uri as http://localhost


        :param str client_id: The OAuth client id of the calling application.
        :param str username: Typically a UPN in the form of an email address.
        """
        self._auth_context.with_token_interactive(client_id, username)
        return self

    def with_username_and_password(self, client_id, username, password):
        # type: (str, str, str) -> "GraphRequest"
        """
        Initializes the client via user credentials
        :param str client_id: The OAuth client id of the calling application.
        :param str username: Typically a UPN in the form of an email address.
        :param str password: The password.
        """
        self._auth_context.with_username_and_password(client_id, username, password)
        return self

    def authenticate_request(self, request):
        # type: (RequestOptions) -> None
        """Authenticate request"""
        token = self._auth_context.acquire_token()
        request.ensure_header("Authorization", "Bearer {0}".format(token.accessToken))

    @property
    def service_root_url(self):
        # type: () -> str
        return "{0}/{1}".format(
            AzureEnvironment.get_graph_authority(self._environment), self._version
        )
