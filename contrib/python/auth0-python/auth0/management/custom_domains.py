from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class CustomDomains:
    """Auth0 custom domains endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/custom-domains"
        if id is not None:
            return url + "/" + id
        return url

    def all(self) -> list[dict[str, Any]]:
        """Retrieves all custom domains.

        See: https://auth0.com/docs/api/management/v2#!/Custom_Domains/get_custom_domains
        """
        return self.client.get(self._url())

    def get(self, id: str) -> dict[str, Any]:
        """Retrieves custom domain.

        See: https://auth0.com/docs/api/management/v2#!/Custom_Domains/get_custom_domains_by_id
        """
        url = self._url("%s" % (id))
        return self.client.get(url)

    def delete(self, id: str) -> Any:
        """Deletes a grant.

        Args:
           id (str): The id of the custom domain to delete.

        See: https://auth0.com/docs/api/management/v2#!/Custom_Domains/delete_custom_domains_by_id
        """
        url = self._url("%s" % (id))
        return self.client.delete(url)

    def create_new(self, body: dict[str, Any]) -> dict[str, Any]:
        """Configure a new custom domain.

        Args:
           body (str): The domain, tye and verification method in json.

        See: https://auth0.com/docs/api/management/v2#!/Custom_Domains/post_custom_domains
        """
        return self.client.post(self._url(), data=body)

    def verify(self, id: str) -> dict[str, Any]:
        """Verify a custom domain.

        Args:
           id (str): The id of the custom domain to delete.

        See: https://auth0.com/docs/api/management/v2#!/Custom_Domains/post_verify
        """
        url = self._url("%s/verify" % (id))
        return self.client.post(url)
