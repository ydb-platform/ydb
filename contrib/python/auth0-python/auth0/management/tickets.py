from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Tickets:
    """Auth0 tickets endpoints

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

    def _url(self, action: str) -> str:
        return f"{self.protocol}://{self.domain}/api/v2/tickets/{action}"

    def create_email_verification(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create an email verification ticket.

        Args:
            body (dict): attributes to set on the email verification request.

        See: https://auth0.com/docs/api/v2#!/Tickets/post_email_verification
        """
        return self.client.post(self._url("email-verification"), data=body)

    def create_pswd_change(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create password change ticket.

        Args:
            body (dict): attributes to set on the password change request.

        See: https://auth0.com/docs/api/v2#!/Tickets/post_password_change
        """
        return self.client.post(self._url("password-change"), data=body)
