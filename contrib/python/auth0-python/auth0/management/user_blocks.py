from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class UserBlocks:
    """Auth0 user blocks endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/user-blocks"
        if id is not None:
            return f"{url}/{id}"
        return url

    def get_by_identifier(self, identifier: str) -> dict[str, Any]:
        """Gets blocks by identifier

        Args:
           identifier (str): Should be any of: username, phone_number, email.

        See: https://auth0.com/docs/api/management/v2#!/User_Blocks/get_user_blocks
        """

        params = {"identifier": identifier}

        return self.client.get(self._url(), params=params)

    def unblock_by_identifier(self, identifier: dict[str, Any]) -> Any:
        """Unblocks by identifier

        Args:
           identifier (str): Should be any of: username, phone_number, email.

        See: https://auth0.com/docs/api/management/v2#!/User_Blocks/delete_user_blocks
        """

        params = {"identifier": identifier}

        return self.client.delete(self._url(), params=params)

    def get(self, id: str) -> dict[str, Any]:
        """Get a user's blocks

        Args:
           id (str): The user_id of the user to retrieve.

        See: https://auth0.com/docs/api/management/v2#!/User_Blocks/get_user_blocks_by_id
        """

        return self.client.get(self._url(id))

    def unblock(self, id: str) -> Any:
        """Unblock a user

        Args:
           id (str): The user_id of the user to update.

        See: https://auth0.com/docs/api/management/v2#!/User_Blocks/delete_user_blocks_by_id
        """

        return self.client.delete(self._url(id))
