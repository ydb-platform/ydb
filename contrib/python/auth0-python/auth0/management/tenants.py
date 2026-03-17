from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Tenants:
    """Auth0 tenants endpoints

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
        return f"{self.protocol}://{self.domain}/api/v2/tenants/settings"

    def get(
        self, fields: list[str] | None = None, include_fields: bool = True
    ) -> dict[str, Any]:
        """Get tenant settings.

        Args:
           fields (list of str, optional): A list of fields to include or
              exclude from the result (depending on include_fields). Leave empty to
              retrieve all fields.

           include_fields (bool, optional): True if the fields specified are
              to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2#!/Tenants/get_settings
        """

        params = {
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
        }

        return self.client.get(self._url(), params=params)

    def update(self, body: dict[str, Any]) -> dict[str, Any]:
        """Update tenant settings.

        Args:
            body (dict): the attributes to update in the tenant.

        See: https://auth0.com/docs/api/v2#!/Tenants/patch_settings
        """
        return self.client.patch(self._url(), data=body)
