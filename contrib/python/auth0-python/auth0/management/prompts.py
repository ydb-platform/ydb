from __future__ import annotations

from typing import Any

from ..rest import RestClient, RestClientOptions
from ..types import TimeoutType


class Prompts:
    """Auth0 prompts endpoints

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

    def _url(self, prompt: str | None = None, language: str | None = None) -> str:
        url = f"{self.protocol}://{self.domain}/api/v2/prompts"
        if prompt is not None and language is not None:
            return f"{url}/{prompt}/custom-text/{language}"
        return url

    def get(self) -> dict[str, Any]:
        """Retrieves prompts settings.

        See: https://auth0.com/docs/api/management/v2#!/Prompts/get_prompts
        """

        return self.client.get(self._url())

    def update(self, body: dict[str, Any]) -> dict[str, Any]:
        """Updates prompts settings.

        See: https://auth0.com/docs/api/management/v2#!/Prompts/patch_prompts
        """

        return self.client.patch(self._url(), data=body)

    def get_custom_text(self, prompt: str, language: str):
        """Retrieves custom text for a prompt in a specific language.

        Args:
            prompt (str): Name of the prompt.

            language (str): Language to update.

        See: https://auth0.com/docs/api/management/v2#!/Prompts/get_custom_text_by_language
        """

        return self.client.get(self._url(prompt, language))

    def update_custom_text(
        self, prompt: str, language: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        """Updates custom text for a prompt in a specific language.

        Args:
            prompt (str): Name of the prompt.

            language (str): Language to update.

            body (dict): An object containing custom dictionaries for a group of screens.

        See: https://auth0.com/docs/api/management/v2#!/Prompts/put_custom_text_by_language
        """

        return self.client.put(self._url(prompt, language), data=body)
