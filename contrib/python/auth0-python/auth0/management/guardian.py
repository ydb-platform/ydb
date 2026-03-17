from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Guardian:
    """Auth0 guardian endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/guardian"
        if id is not None:
            return f"{url}/{id}"
        return url

    def all_factors(self) -> list[dict[str, Any]]:
        """Retrieves all factors. Useful to check factor enablement and
             trial status.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/get_factors
        """

        return self.client.get(self._url("factors"))

    def update_factor(self, name: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update Guardian factor.
        Useful to enable / disable factor.

        Args:
            name (str): Either push-notification or sms.

            body (dict): Attributes to modify.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/put_factors_by_name
        """
        url = self._url(f"factors/{name}")
        return self.client.put(url, data=body)

    def update_templates(self, body: dict[str, Any]) -> dict[str, Any]:
        """Update enrollment and verification SMS templates.

        Useful to send custom messages on sms enrollment and verification.

        Args:
            body (dict): Attributes to modify.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/put_templates
        """

        return self.client.put(self._url("factors/sms/templates"), data=body)

    def get_templates(self) -> dict[str, Any]:
        """Get enrollment and verification templates.

        Retrieve both templates. Useful to check if a different template than
            default was set.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/get_templates
        """

        return self.client.get(self._url("factors/sms/templates"))

    def get_enrollment(self, id: str) -> dict[str, Any]:
        """Retrieves an enrollment.
        Useful to check its type and related metadata.

        Args:
           id (str): The id of the device account to update.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/get_enrollments_by_id
        """
        url = self._url(f"enrollments/{id}")
        return self.client.get(url)

    def delete_enrollment(self, id: str) -> Any:
        """Deletes an enrollment.

        Useful when you want to force re-enroll.

        Args:
           id (str): The id of the device account to update.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/delete_enrollments_by_id
        """
        url = self._url(f"enrollments/{id}")
        return self.client.delete(url)

    def create_enrollment_ticket(self, body: dict[str, Any]) -> dict[str, Any]:
        """Creates an enrollment ticket for user_id

        A useful way to send an email to a user, with a link that lead to
            start the enrollment process.

        Args:
            body (dict): Details of the user to send the ticket to.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/post_ticket
        """
        return self.client.post(self._url("enrollments/ticket"), data=body)

    def get_factor_providers(self, factor_name: str, name: str) -> dict[str, Any]:
        """Get Guardian SNS or SMS factor providers.

        Returns provider configuration.

        Args:
           factor_name (str): Either push-notification or sms.

           name (str): Name of the provider.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/get_sns
             https://auth0.com/docs/api/management/v2#!/Guardian/get_twilio
        """
        url = self._url(f"factors/{factor_name}/providers/{name}")
        return self.client.get(url)

    def update_factor_providers(
        self, factor_name: str, name: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Get Guardian factor providers.

        Returns provider configuration.

        Args:
           factor_name (str): Either push-notification or sms.

           name (str): Name of the provider.

           body (dict): Details of the factor provider.

        See: https://auth0.com/docs/api/management/v2#!/Guardian/put_twilio
        """
        url = self._url(f"factors/{factor_name}/providers/{name}")
        return self.client.put(url, data=body)
