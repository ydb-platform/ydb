from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class EmailTemplates:
    """Auth0 email templates endpoints

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
        url = f"{self.protocol}://{self.domain}/api/v2/email-templates"
        if id is not None:
            return f"{url}/{id}"
        return url

    def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new email template.

        Args:
           body (dict): Attributes for the new email template.

        See: https://auth0.com/docs/api/management/v2#!/Email_Templates/post_email_templates
        """

        return self.client.post(self._url(), data=body)

    def get(self, template_name: str) -> dict[str, Any]:
        """Retrieves an email template by its name.

        Args:
           template_name (str): Name of the email template to get.
              Must be one of: 'verify_email', 'reset_email', 'welcome_email',
              'blocked_account', 'stolen_credentials', 'enrollment_email',
              'change_password', 'password_reset', 'mfa_oob_code'.

        See: https://auth0.com/docs/api/management/v2#!/Email_Templates/get_email_templates_by_templateName
        """

        return self.client.get(self._url(template_name))

    def update(self, template_name: str, body: dict[str, Any]) -> dict[str, Any]:
        """Update an existing email template.

        Args:
           template_name (str): Name of the email template to update.
              Must be one of: 'verify_email', 'reset_email', 'welcome_email',
              'blocked_account', 'stolen_credentials', 'enrollment_email',
              'change_password', 'password_reset', 'mfa_oob_code'.

           body (dict): Attributes to update on the email template.

        See: https://auth0.com/docs/api/management/v2#!/Email_Templates/patch_email_templates_by_templateName
        """

        return self.client.patch(self._url(template_name), data=body)
