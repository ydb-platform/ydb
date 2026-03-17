from __future__ import annotations

from typing import Any, List  # List is being used as list is already a method.

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class SelfServiceProfiles:
    """Auth0 Self Service Profiles endpoints

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

    def _url(self, profile_id: str | None = None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/self-service-profiles"
        if profile_id is not None:
            return f"{url}/{profile_id}"
        return url

    def all(
        self,
        page: int = 0,
        per_page: int = 25,
        include_totals: bool = True,
    ) -> List[dict[str, Any]]:
        """List self-service profiles.

        Args:
            page (int, optional): The result's page number (zero based). By default,
                retrieves the first page of results.

            per_page (int, optional): The amount of entries per page. By default,
                retrieves 25 results per page.

            include_totals (bool, optional): True if the query summary is
                to be included in the result, False otherwise. Defaults to True.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/get-self-service-profiles
        """

        params = {
            "page": page,
            "per_page": per_page,
            "include_totals": str(include_totals).lower(),
        }

        return self.client.get(self._url(), params=params)

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new self-service profile.

        Args:
            body (dict): Attributes for the new self-service profile.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/post-self-service-profiles
        """

        return self.client.post(self._url(), data=body)

    def get(self, profile_id: str) -> dict[str, Any]:
        """Get a self-service profile.

        Args:
            id (str): The id of the self-service profile to retrieve.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/get-self-service-profiles-by-id
        """

        return self.client.get(self._url(profile_id))

    def delete(self, profile_id: str) -> None:
        """Delete a self-service profile.

        Args:
            id (str): The id of the self-service profile to delete.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/delete-self-service-profiles-by-id
        """

        self.client.delete(self._url(profile_id))

    def update(self, profile_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update a self-service profile.

        Args:
            id (str): The id of the self-service profile to update.

            body (dict): Attributes of the self-service profile to modify.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/patch-self-service-profiles-by-id
        """

        return self.client.patch(self._url(profile_id), data=body)

    def get_custom_text(
        self, profile_id: str, language: str, page: str
    ) -> dict[str, Any]:
        """Get the custom text for a self-service profile.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/get-self-service-profile-custom-text
        """

        url = self._url(f"{profile_id}/custom-text/{language}/{page}")
        return self.client.get(url)

    def update_custom_text(
        self, profile_id: str, language: str, page: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Update the custom text for a self-service profile.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/put-self-service-profile-custom-text
        """

        url = self._url(f"{profile_id}/custom-text/{language}/{page}")
        return self.client.put(url, data=body)

    def create_sso_ticket(
        self, profile_id: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Create a single sign-on ticket for a self-service profile.

        Args:
            id (str): The id of the self-service profile to create the ticket for.

            body (dict): Attributes for the single sign-on ticket.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/post-sso-ticket
        """

        url = self._url(f"{profile_id}/sso-ticket")
        return self.client.post(url, data=body)

    def revoke_sso_ticket(self, profile_id: str, ticket_id: str) -> None:
        """Revoke a single sign-on ticket for a self-service profile.

        Args:
            id (str): The id of the self-service profile to revoke the ticket from.

            ticket (str): The ticket to revoke.

        See: https://auth0.com/docs/api/management/v2/self-service-profiles/post-revoke
        """

        url = self._url(f"{profile_id}/sso-ticket/{ticket_id}/revoke")
        self.client.post(url)