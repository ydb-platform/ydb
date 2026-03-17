from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class ClientGrants:
    """Auth0 client grants endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/client-grants"
        if id is not None:
            return f"{url}/{id}"
        return url

    def all(
        self,
        audience: str | None = None,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
        client_id: str | None = None,
        allow_any_organization: bool | None = None,
    ):
        """Retrieves all client grants.

        Args:
            audience (str, optional): URL encoded audience of a Resource Server
                to filter.

            page (int, optional): The result's page number (zero based). When not set,
                the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
                the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to False.

            client_id (string, optional): The id of a client to filter.

            allow_any_organization (bool, optional): Optional filter on allow_any_organization.

        See: https://auth0.com/docs/api/management/v2#!/Client_Grants/get_client_grants
        """

        params = {
            "audience": audience,
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
            "client_id": client_id,
            "allow_any_organization": allow_any_organization,
        }

        return self.client.get(self._url(), params=params)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Creates a client grant.

        Args:
           body (dict): Attributes for the new client grant.

        See: https://auth0.com/docs/api/management/v2#!/Client_Grants/post_client_grants
        """

        return self.client.post(self._url(), data=body)

    def delete(self, id: str) -> Any:
        """Deletes a client grant.

        Args:
           id (str): Id of client grant to delete.

        See: https://auth0.com/docs/api/management/v2#!/Client_Grants/delete_client_grants_by_id
        """

        return self.client.delete(self._url(id))

    def update(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Modifies a client grant.

        Args:
           id (str): The id of the client grant to modify.

           body (dict): Attributes to update.

        See: https://auth0.com/docs/api/management/v2#!/Client_Grants/patch_client_grants_by_id
        """

        return self.client.patch(self._url(id), data=body)

    def get_organizations(
        self,
        id: str,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
        from_param: str | None = None,
        take: int | None = None,
    ):
        """Get the organizations associated to a client grant.

        Args:
            id (str): Id of client grant.

            page (int, optional): The result's page number (zero based). When not set,
                the default value is up to the server.

            per_page (int, optional): The amount of entries per page. When not set,
                the default value is up to the server.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to False.

            from_param (str, optional): Id to start retrieving entries. You can
                limit the amount of entries using the take parameter.

            take (int, optional): The total amount of entries to retrieve when
                using the from parameter. When not set, the default value is up to the server.
        """

        params = {
            "per_page": per_page,
            "page": page,
            "include_totals": str(include_totals).lower(),
            "from": from_param,
            "take": take,
        }

        return self.client.get(self._url(f"{id}/organizations"), params=params)
