from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class UsersByEmail:
    """Auth0 users by email endpoints

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

    def _url(self) -> str:
        return f"{self.protocol}://{self.domain}/api/v2/users-by-email"

    def search_users_by_email(
        self, email: str, fields: list[str] | None = None, include_fields: bool = True
    ) -> list[dict[str, Any]]:
        """List or search users.

        Args:

            email: Email to search.

            fields (list of str, optional): A list of fields to include or
                exclude from the result (depending on include_fields). Leave empty to
                retrieve all fields.

            include_fields (bool, optional): True if the fields specified are
                to be include in the result, False otherwise.

        See: https://auth0.com/docs/api/management/v2#!/Users_By_Email/get_users_by_email
        """
        params = {
            "email": email,
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
        }
        return self.client.get(self._url(), params=params)
