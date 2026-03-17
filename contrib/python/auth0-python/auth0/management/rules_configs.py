from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class RulesConfigs:
    """RulesConfig endpoint implementation.

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
        url = f"{self.protocol}://{self.domain}/api/v2/rules-configs"
        if id is not None:
            return url + "/" + id
        return url

    def all(self) -> list[dict[str, Any]]:
        """Lists the config variable keys for rules.

        See: https://auth0.com/docs/api/management/v2#!/Rules_Configs/get_rules_configs
        """
        return self.client.get(self._url())

    def unset(self, key: str) -> Any:
        """Removes the rules config for a given key.

        Args:
            key (str): rules config key to remove.

        See: https://auth0.com/docs/api/management/v2#!/Rules_Configs/delete_rules_configs_by_key
        """
        return self.client.delete(self._url(key))

    def set(self, key: str, value: str) -> dict[str, Any]:
        """Sets the rules config for a given key.

        Args:
            key (str): rules config key to set.

            value (str): value to set for the rules config key.

        See: https://auth0.com/docs/api/management/v2#!/Rules_Configs/put_rules_configs_by_key
        """
        url = self._url(f"{key}")
        body = {"value": value}
        return self.client.put(url, data=body)
