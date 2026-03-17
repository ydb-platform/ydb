from __future__ import annotations

from typing import Any, List  # List is being used as list is already a method.

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class NetworkAcls:
    """Auth0 Netwrok Acls endpoints

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

    def _url(self, id: str | None = None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/network-acls"
        if id is not None:
            return f"{url}/{id}"
        return url

    def all(
        self,
        page: int = 0,
        per_page: int = 25,
        include_totals: bool = True,
    ) -> List[dict[str, Any]]:
        """List self-service profiles.

        Args:
            page (int, optional): The result's page number (zero based). By default,
                retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
                retrieves 25 results per page.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2/network-acls/get-network-acls
        """

        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }

        return self.client.get(self._url(), params=params)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new self-service profile.

        Args:
            body (dict): Attributes for the new access control list.

        See: https://auth0.com/docs/api/management/v2/network-acls/post-network-acls
        """

        return self.client.post(self._url(), data=body)

    def get(self, id: str) -> dict[str, Any]:
        """Get a self-service profile.

        Args:
            id (str): The id of the access control list to retrieve.

        See: https://auth0.com/docs/api/management/v2/network-acls/get-network-acls-by-id
        """

        return self.client.get(self._url(id))

    def delete(self, id: str) -> None:
        """Delete a self-service profile.

        Args:
            id (str): The id of the access control list to delete.

        See: https://auth0.com/docs/api/management/v2/network-acls/delete-network-acls-by-id
        """

        self.client.delete(self._url(id))

    def update(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update a access control list.

        Args:
            id (str): The id of the access control list to update.

            body (dict): Attributes of the access control list to modify.

        See: https://auth0.com/docs/api/management/v2/network-acls/put-network-acls-by-id
        """

        return self.client.put(self._url(id), data=body)
    
    def update_partial(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update partially the access control list.

        See: https://auth0.com/docs/api/management/v2/network-acls/patch-network-acls-by-id
        """

        return self.client.patch(self._url(id), data=body)

    