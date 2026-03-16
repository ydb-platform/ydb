"""
Prompt management for PostHog AI SDK.

Fetch and compile LLM prompts from PostHog with caching and fallback support.
"""

import logging
import re
import time
import urllib.parse
from typing import Any, Dict, Optional, Union

from posthog.request import USER_AGENT, _get_session
from posthog.utils import remove_trailing_slash

log = logging.getLogger("posthog")

APP_ENDPOINT = "https://us.posthog.com"
DEFAULT_CACHE_TTL_SECONDS = 300  # 5 minutes

PromptVariables = Dict[str, Union[str, int, float, bool]]


class CachedPrompt:
    """Cached prompt with metadata."""

    def __init__(self, prompt: str, fetched_at: float):
        self.prompt = prompt
        self.fetched_at = fetched_at


def _is_prompt_api_response(data: Any) -> bool:
    """Check if the response is a valid prompt API response."""
    return (
        isinstance(data, dict)
        and "prompt" in data
        and isinstance(data.get("prompt"), str)
    )


class Prompts:
    """
    Fetch and compile LLM prompts from PostHog.

    Can be initialized with a PostHog client or with direct options.

    Examples:
        ```python
        from posthog import Posthog
        from posthog.ai.prompts import Prompts

        # With PostHog client
        posthog = Posthog('phc_xxx', host='https://us.posthog.com', personal_api_key='phx_xxx')
        prompts = Prompts(posthog)

        # Or with direct options (no PostHog client needed)
        prompts = Prompts(
            personal_api_key='phx_xxx',
            project_api_key='phc_xxx',
            host='https://us.posthog.com',
        )

        # Fetch with caching and fallback
        template = prompts.get('support-system-prompt', fallback='You are a helpful assistant.')

        # Compile with variables
        system_prompt = prompts.compile(template, {
            'company': 'Acme Corp',
            'tier': 'premium',
        })
        ```
    """

    def __init__(
        self,
        posthog: Optional[Any] = None,
        *,
        personal_api_key: Optional[str] = None,
        project_api_key: Optional[str] = None,
        host: Optional[str] = None,
        default_cache_ttl_seconds: Optional[int] = None,
    ):
        """
        Initialize Prompts.

        Args:
            posthog: PostHog client instance (optional if personal_api_key provided)
            personal_api_key: Direct personal API key (optional if posthog provided)
            project_api_key: Direct project API key (optional if posthog provided)
            host: PostHog host (defaults to app endpoint)
            default_cache_ttl_seconds: Default cache TTL (defaults to 300)
        """
        self._default_cache_ttl_seconds = (
            default_cache_ttl_seconds or DEFAULT_CACHE_TTL_SECONDS
        )
        self._cache: Dict[str, CachedPrompt] = {}

        if posthog is not None:
            self._personal_api_key = getattr(posthog, "personal_api_key", None) or ""
            self._project_api_key = getattr(posthog, "api_key", None) or ""
            self._host = remove_trailing_slash(
                getattr(posthog, "raw_host", None) or APP_ENDPOINT
            )
        else:
            self._personal_api_key = personal_api_key or ""
            self._project_api_key = project_api_key or ""
            self._host = remove_trailing_slash(host or APP_ENDPOINT)

    def get(
        self,
        name: str,
        *,
        cache_ttl_seconds: Optional[int] = None,
        fallback: Optional[str] = None,
    ) -> str:
        """
        Fetch a prompt by name from the PostHog API.

        Caching behavior:
        1. If cache is fresh, return cached value
        2. If fetch fails and cache exists (stale), return stale cache with warning
        3. If fetch fails and fallback provided, return fallback with warning
        4. If fetch fails with no cache/fallback, raise exception

        Args:
            name: The name of the prompt to fetch
            cache_ttl_seconds: Cache TTL in seconds (defaults to instance default)
            fallback: Fallback prompt to use if fetch fails and no cache available

        Returns:
            The prompt string

        Raises:
            Exception: If the prompt cannot be fetched and no fallback is available
        """
        ttl = (
            cache_ttl_seconds
            if cache_ttl_seconds is not None
            else self._default_cache_ttl_seconds
        )

        # Check cache first
        cached = self._cache.get(name)
        now = time.time()

        if cached is not None:
            is_fresh = (now - cached.fetched_at) < ttl

            if is_fresh:
                return cached.prompt

        # Try to fetch from API
        try:
            prompt = self._fetch_prompt_from_api(name)
            fetched_at = time.time()

            # Update cache
            self._cache[name] = CachedPrompt(prompt=prompt, fetched_at=fetched_at)

            return prompt

        except Exception as error:
            # Fallback order:
            # 1. Return stale cache (with warning)
            if cached is not None:
                log.warning(
                    '[PostHog Prompts] Failed to fetch prompt "%s", using stale cache: %s',
                    name,
                    error,
                )
                return cached.prompt

            # 2. Return fallback (with warning)
            if fallback is not None:
                log.warning(
                    '[PostHog Prompts] Failed to fetch prompt "%s", using fallback: %s',
                    name,
                    error,
                )
                return fallback

            # 3. Raise error
            raise

    def compile(self, prompt: str, variables: PromptVariables) -> str:
        """
        Replace {{variableName}} placeholders with values.

        Unmatched variables are left unchanged.
        Supports variable names with hyphens and dots (e.g., user-id, company.name).

        Args:
            prompt: The prompt template string
            variables: Object containing variable values

        Returns:
            The compiled prompt string
        """

        def replace_variable(match: re.Match) -> str:
            variable_name = match.group(1)

            if variable_name in variables:
                return str(variables[variable_name])

            return match.group(0)

        return re.sub(r"\{\{([\w.-]+)\}\}", replace_variable, prompt)

    def clear_cache(self, name: Optional[str] = None) -> None:
        """
        Clear cached prompts.

        Args:
            name: Specific prompt to clear. If None, clears all cached prompts.
        """
        if name is not None:
            self._cache.pop(name, None)
        else:
            self._cache.clear()

    def _fetch_prompt_from_api(self, name: str) -> str:
        """
        Fetch prompt from PostHog API.

        Endpoint: {host}/api/environments/@current/llm_prompts/name/{encoded_name}/?token={encoded_project_api_key}
        Auth: Bearer {personal_api_key}

        Args:
            name: The name of the prompt to fetch

        Returns:
            The prompt string

        Raises:
            Exception: If the prompt cannot be fetched
        """
        if not self._personal_api_key:
            raise Exception(
                "[PostHog Prompts] personal_api_key is required to fetch prompts. "
                "Please provide it when initializing the Prompts instance."
            )
        if not self._project_api_key:
            raise Exception(
                "[PostHog Prompts] project_api_key is required to fetch prompts. "
                "Please provide it when initializing the Prompts instance."
            )

        encoded_name = urllib.parse.quote(name, safe="")
        encoded_project_api_key = urllib.parse.quote(self._project_api_key, safe="")
        url = f"{self._host}/api/environments/@current/llm_prompts/name/{encoded_name}/?token={encoded_project_api_key}"

        headers = {
            "Authorization": f"Bearer {self._personal_api_key}",
            "User-Agent": USER_AGENT,
        }

        response = _get_session().get(url, headers=headers, timeout=10)

        if not response.ok:
            if response.status_code == 404:
                raise Exception(f'[PostHog Prompts] Prompt "{name}" not found')

            if response.status_code == 403:
                raise Exception(
                    f'[PostHog Prompts] Access denied for prompt "{name}". '
                    "Check that your personal_api_key has the correct permissions and the LLM prompts feature is enabled."
                )

            raise Exception(
                f'[PostHog Prompts] Failed to fetch prompt "{name}": HTTP {response.status_code}'
            )

        try:
            data = response.json()
        except Exception:
            raise Exception(
                f'[PostHog Prompts] Invalid response format for prompt "{name}"'
            )

        if not _is_prompt_api_response(data):
            raise Exception(
                f'[PostHog Prompts] Invalid response format for prompt "{name}"'
            )

        return data["prompt"]
