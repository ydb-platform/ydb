from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class AttackProtection:
    """Auth0 attack protection endpoints

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

    def _url(self, component: str) -> str:
        return "{}://{}/api/v2/attack-protection/{}".format(
            self.protocol, self.domain, component
        )

    def get_breached_password_detection(self) -> dict[str, Any]:
        """Get breached password detection settings.

        Returns the breached password detection settings.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/get_breached_password_detection
        """
        url = self._url("breached-password-detection")
        return self.client.get(url)

    def update_breached_password_detection(
        self, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Update breached password detection settings.

        Returns the breached password detection settings.

        Args:

           body (dict): breached password detection settings.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/patch_breached_password_detection
        """
        url = self._url("breached-password-detection")
        return self.client.patch(url, data=body)

    def get_brute_force_protection(self) -> dict[str, Any]:
        """Get the brute force configuration.

        Returns the brute force configuration.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/get_brute_force_protection
        """
        url = self._url("brute-force-protection")
        return self.client.get(url)

    def update_brute_force_protection(self, body: dict[str, Any]) -> dict[str, Any]:
        """Update the brute force configuration.

        Returns the brute force configuration.

        Args:

           body (dict): updates of the brute force configuration.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/patch_brute_force_protection
        """
        url = self._url("brute-force-protection")
        return self.client.patch(url, data=body)

    def get_suspicious_ip_throttling(self) -> dict[str, Any]:
        """Get the suspicious IP throttling configuration.

        Returns the suspicious IP throttling configuration.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/get_suspicious_ip_throttling
        """
        url = self._url("suspicious-ip-throttling")
        return self.client.get(url)

    def update_suspicious_ip_throttling(self, body: dict[str, Any]) -> dict[str, Any]:
        """Update the suspicious IP throttling configuration.

        Returns the suspicious IP throttling configuration.

        Args:

           body (dict): updates of the suspicious IP throttling configuration.

        See: https://auth0.com/docs/api/management/v2#!/Attack_Protection/patch_suspicious_ip_throttling
        """
        url = self._url("suspicious-ip-throttling")
        return self.client.patch(url, data=body)
