from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Connections:
    """Auth0 connection endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/connections"
        if id is not None:
            return f"{url}/{id}"
        return url

    def all(
        self,
        strategy: str | None = None,
        fields: list[str] | None = None,
        include_fields: bool = True,
        page: int | None = None,
        per_page: int | None = None,
        extra_params: dict[str, Any] | None = None,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieves all connections.

        Args:
           strategy (str, optional): Only retrieve connections of
              this strategy type. (e.g: strategy='amazon')

           fields (list of str, optional): A list of fields to include or
              exclude from the result (depending on include_fields). Leave empty to
              retrieve all fields. By default, all the fields will be retrieved.

           include_fields (bool, optional): True if the fields specified are
              to be included in the result, False otherwise. Defaults to True.

           page (int): The result's page number (zero based). When not set,
              the default value is up to the server.

           per_page (int, optional): The amount of entries per page. When not set,
              the default value is up to the server.

           extra_params (dictionary, optional): The extra parameters to add to
             the request. The fields, include_fields, page and per_page values
             specified as parameters take precedence over the ones defined here.

           name (str): Provide the name of the connection to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/Connections/get_connections

        Returns:
           A list of connection objects.
        """

        params = extra_params or {}
        params["strategy"] = strategy or None
        params["fields"] = fields and ",".join(fields) or None
        params["include_fields"] = str(include_fields).lower()
        params["page"] = page
        params["per_page"] = per_page
        params["name"] = name

        return self.client.get(self._url(), params=params)

    def get(
        self, id: str, fields: list[str] | None = None, include_fields: bool = True
    ) -> dict[str, Any]:
        """Retrieve connection by id.

        Args:
           id (str): Id of the connection to get.

           fields (list of str, optional): A list of fields to include or
              exclude from the result (depending on include_fields). Leave empty to
              retrieve all fields. By default, all the fields will be retrieved.

           include_fields (bool, optional): True if the fields specified are
              to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2#!/Connections/get_connections_by_id

        Returns:
            A connection object.
        """

        params = {
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
        }

        return self.client.get(self._url(id), params=params)

    def delete(self, id: str) -> Any:
        """Deletes a connection and all its users.

        Args:
           id: Id of the connection to delete.

        See: https://auth0.com/docs/api/management/v2#!/Connections/delete_connections_by_id

        Returns:
           An empty dict.
        """

        return self.client.delete(self._url(id))

    def update(self, id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Modifies a connection.

        Args:
           id: Id of the connection.

           body (dict): Specifies which fields are to be modified, and to what values.

        See: https://auth0.com/docs/api/management/v2#!/Connections/patch_connections_by_id

        Returns:
           The modified connection object.
        """

        return self.client.patch(self._url(id), data=body)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Creates a new connection.

        Args:
            body (dict): Attributes used to create the connection. Mandatory
                attributes are: 'name' and 'strategy'.

        See: https://auth0.com/docs/api/management/v2#!/Connections/post_connections
        """

        return self.client.post(self._url(), data=body)

    def delete_user_by_email(self, id: str, email: str) -> Any:
        """Deletes a specified connection user by its email.

        Args:
           id (str): The id of the connection (must be a database connection).

           email (str): The email of the user to delete.

        See: https://auth0.com/docs/api/management/v2#!/Connections/delete_users_by_email

        Returns:
            An empty dict.
        """
        return self.client.delete(self._url(id) + "/users", params={"email": email})
