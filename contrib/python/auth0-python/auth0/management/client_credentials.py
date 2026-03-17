from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class ClientCredentials:
    """Auth0 client credentials endpoints.

    Args:
        domain (str): Your Auth0 domain, e.g: 'username.auth0.com'

        token (str): Management API v2 Token

        telemetry (bool, optional): Enable or disable Telemetry
            (defaults to True)

        timeout (float or tuple, optional): Change the requests
            connect and read timeout. Pass a tuple to specify
            both values separately or a float to set both to it.
            (defaults to 5.0 for both)

        protocol (str, optional): Protocol to use when making requests.
            (defaults to "https")

        rest_options (RestClientOptions): Pass an instance of
            RestClientOptions to configure additional RestClient
            options, such as rate-limit retries.
            (defaults to None)
    """

    def __init__(
        self,
        domain: str,
        token: str,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        protocol: str = "https",
        rest_options: RestClientOptions | None = None,
    ) -> None:
        self.domain = domain
        self.protocol = protocol
        self.client = RestClient(
            jwt=token, telemetry=telemetry, timeout=timeout, options=rest_options
        )

    def _url(self, client_id: str, id: str | None = None) -> str:
        url = "{}://{}/api/v2/clients/{}/credentials".format(
            self.protocol, self.domain, client_id
        )
        if id is not None:
            return f"{url}/{id}"
        return url

    def all(self, client_id: str) -> list[dict[str, Any]]:
        """Get a list of credentials associated with a client.

        Args:
            client_id (string): The id of a client that owns the credentials.

        See: https://auth0.com/docs/api/management/v2#!/Client_Credentials/get_client_credentials
        """
        return self.client.get(self._url(client_id))

    def get(self, client_id: str, id: str) -> dict[str, Any]:
        """Retrieve a specified client credential.

        Args:
            client_id (string): The id of a client that owns the credential.

            id (string): The id of the credential.

        See: https://auth0.com/docs/api/management/v2#!/Client_Credentials/get_client_credentials_by_id
        """
        return self.client.get(self._url(client_id, id))

    def create(self, client_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Create a credential on a client.

        Args:
            client_id (string): The id of a client to create the credential for.

        See: https://auth0.com/docs/api/management/v2#!/Client_Credentials/post_client_credentials
        """
        return self.client.post(self._url(client_id), data=body)

    def delete(self, client_id: str, id: str) -> dict[str, Any]:
        """Delete a client's credential.

        Args:
           id (str): The id of credential to delete.

        See: https://auth0.com/docs/api/management/v2#!/Client_Credentials/delete_client_credentials_by_id
        """

        return self.client.delete(self._url(client_id, id))
