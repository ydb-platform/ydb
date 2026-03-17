from __future__ import annotations

from typing import Any

from auth0.rest import RestClient, RestClientOptions
from auth0.types import TimeoutType


class Users:
    """Users client.

    Args:
        domain (str): The domain of your Auth0 tenant
        telemetry (bool, optional): Enable or disable telemetry (defaults to True)
        timeout (float or tuple, optional): Change the requests connect and read timeout. Pass a tuple to specify both values separately or a float to set both to it. (defaults to 5.0 for both)
        protocol (str, optional): Useful for testing. (defaults to 'https')
    """

    def __init__(
        self,
        domain: str,
        telemetry: bool = True,
        timeout: TimeoutType = 5.0,
        protocol: str = "https",
    ) -> None:
        self.domain = domain
        self.protocol = protocol
        self.client = RestClient(
            None,
            options=RestClientOptions(telemetry=telemetry, timeout=timeout, retries=0),
        )

    """Userinfo related endpoints.

    Args:
        domain (str): Your auth0 domain (e.g: username.auth0.com)
    """

    def userinfo(self, access_token: str) -> dict[str, Any]:
        """Returns the user information based on the Auth0 access token.
        This endpoint will work only if openid was granted as a scope for the access_token.

        Args:
            access_token (str): Auth0 access token (obtained during login).

        Returns:
            The user profile.
        """
        data: dict[str, Any] = self.client.get(
            url=f"{self.protocol}://{self.domain}/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        return data
