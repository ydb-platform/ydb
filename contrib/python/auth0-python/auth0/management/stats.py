from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Stats:
    """Auth0 stats endpoints

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
        return f"{self.protocol}://{self.domain}/api/v2/stats/{action}"

    def active_users(self) -> int:
        """Gets the active users count (logged in during the last 30 days).

        Returns: An integer.

        See: https://auth0.com/docs/api/management/v2#!/Stats/get_active_users
        """

        return self.client.get(self._url("active-users"))

    def daily_stats(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> list[dict[str, Any]]:
        """Gets the daily stats for a particular period.

        Args:
           from_date (str, optional): The first day of the period (inclusive) in
              YYYYMMDD format.

           to_date (str, optional): The last day of the period (inclusive) in
              YYYYMMDD format.

        See: https://auth0.com/docs/api/management/v2#!/Stats/get_daily
        """

        return self.client.get(
            self._url("daily"), params={"from": from_date, "to": to_date}
        )
