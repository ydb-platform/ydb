from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class DeviceCredentials:
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
        url = f"{self.protocol}://{self.domain}/api/v2/device-credentials"
        if id is not None:
            return f"{url}/{id}"
        return url

    def get(
        self,
        user_id: str,
        client_id: str,
        type: str,
        fields: list[str] | None = None,
        include_fields: bool = True,
        page: int | None = None,
        per_page: int | None = None,
        include_totals: bool = False,
    ):
        """List device credentials.

        Args:
            user_id (str): The user_id of the devices to retrieve.

            client_id (str): The client_id of the devices to retrieve.

            type (str): The type of credentials (public_key, refresh_token).

            fields (list, optional): A list of fields to include or exclude
                (depending on include_fields) from the result. Leave empty to
                retrieve all fields.

            include_fields (bool, optional): True if the fields specified are
                to be included in the result, False otherwise. Defaults to True.

            page (int, optional): Page index of the results to return. First page is 0.

            per_page (int, optional): Number of results per page.

            include_totals (bool, optional): True to return results inside an object
                that contains the total result count (True) or as a direct array of
                results (False, default).

        See: https://auth0.com/docs/api/management/v2#!/Device_Credentials/get_device_credentials
        """

        params = {
            "fields": fields and ",".join(fields) or None,
            "include_fields": str(include_fields).lower(),
            "user_id": user_id,
            "client_id": client_id,
            "type": type,
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }
        return self.client.get(self._url(), params=params)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a device public key.

        Args:
            body (dict): parameters for creating the public key (e.g: type,
                device_name, client_id, etc).

        See: https://auth0.com/docs/api/v2#!/Device_Credentials/post_device_credentials
        """
        return self.client.post(self._url(), data=body)

    def delete(self, id: str) -> Any:
        """Delete credential.

        Args:
            id (str):  The id of the credential to delete.

        See: https://auth0.com/docs/api/management/v2#!/Device_Credentials/delete_device_credentials_by_id
        """
        return self.client.delete(self._url(id))
