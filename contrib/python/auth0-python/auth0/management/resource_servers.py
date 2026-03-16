from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class ResourceServers:
    """Auth0 resource servers endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/resource-servers"
        if id is not None:
            return f"{url}/{id}"
        return url

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new resource server.

        Args:
           body (dict): Attributes for the new resource Server.

        See: https://auth0.com/docs/api/management/v2#!/Resource_Servers/post_resource_servers
        """

        return self.client.post(self._url(), data=body)

    def get_all(
        self,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
    ):
        """Retrieves all resource servers

        Args:
            page (int, optional): The result's page number (zero based). When not set,
              the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
              the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to False.


        See: https://auth0.com/docs/api/management/v2#!/Resource_Servers/get_resource_servers
        """

        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }

        return self.client.get(self._url(), params=params)

    def get(self, id: str) -> dict[str, Any]:
        """Retrieves a resource server by its id.

        Args:
           id (str): id of the resource server to get.


        See: https://auth0.com/docs/api/management/v2#!/Resource_Servers/get_resource_servers_by_id
        """

        return self.client.get(self._url(id))

    def delete(self, id: str) -> Any:
        """Deletes a resource server.

        Args:
           id (str): Id of resource server to delete.


        See: https://auth0.com/docs/api/management/v2#!/Resource_Servers/delete_resource_servers_by_id
        """

        return self.client.delete(self._url(id))

    def update(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Modifies a resource server.

        Args:
           id (str): The id of the resource server to update.

           body (dict): Attributes to modify.

        See: https://auth0.com/docs/api/management/v2#!/Resource_Servers/patch_resource_servers_by_id
        """

        return self.client.patch(self._url(id), data=body)
