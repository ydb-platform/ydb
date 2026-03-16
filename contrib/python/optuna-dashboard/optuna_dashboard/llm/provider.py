from __future__ import annotations

from typing import Protocol


class RateLimitExceeded(Exception):
    """Raised when the request rate limit is exceeded."""


class InvalidAuthentication(Exception):
    """Raised when authentication fails, for example due to an incorrect API key."""


class LLMProvider(Protocol):
    """An abstract interface for LLM providers.

    .. warning::
        This protocol is not yet stable. Implementing custom LLM providers
        based on this interface is discouraged, as future changes may break
        your implementation.
    """

    def call(self, prompt: str) -> str:
        """Call a prompt to the LLM provider and return the generated response.

        Args:
            prompt: The input text prompt to send to the LLM providers.

        Returns:
            The model-generated response as a string.

        Raises:
            RateLimitExceeded: if LLM provider returns too many requests error.
            InvalidAuthentication: if authentication fails.
        """
        ...
