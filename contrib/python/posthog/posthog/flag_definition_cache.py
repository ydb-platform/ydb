"""
Flag Definition Cache Provider interface for multi-worker environments.

EXPERIMENTAL: This API may change in future minor version bumps.

This module provides an interface for external caching of feature flag definitions,
enabling multi-worker environments (Kubernetes, load-balanced servers, serverless
functions) to share flag definitions and reduce API calls.

Usage:

    from posthog import Posthog
    from posthog.flag_definition_cache import FlagDefinitionCacheProvider

    cache = RedisFlagDefinitionCache(redis_client, "my-team")
    posthog = Posthog(
        "<project_api_key>",
        personal_api_key="<personal_api_key>",
        flag_definition_cache_provider=cache,
    )
"""

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from typing_extensions import Required, TypedDict


class FlagDefinitionCacheData(TypedDict):
    """
    Data structure for cached flag definitions.

    Attributes:
        flags: List of feature flag definition dictionaries from the API.
        group_type_mapping: Mapping of group type indices to group names.
        cohorts: Dictionary of cohort definitions for local evaluation.
    """

    flags: Required[List[Dict[str, Any]]]
    group_type_mapping: Required[Dict[str, str]]
    cohorts: Required[Dict[str, Any]]


@runtime_checkable
class FlagDefinitionCacheProvider(Protocol):
    """
    Interface for external caching of feature flag definitions.

    Enables multi-worker environments to share flag definitions, reducing API
    calls while ensuring all workers have consistent data.

    EXPERIMENTAL: This API may change in future minor version bumps.

    The four methods handle the complete lifecycle of flag definition caching:

    1. `should_fetch_flag_definitions()` - Called before each poll to determine
       if this worker should fetch new definitions. Use for distributed lock
       coordination to ensure only one worker fetches at a time.

    2. `get_flag_definitions()` - Called when `should_fetch_flag_definitions()`
       returns False. Returns cached definitions if available.

    3. `on_flag_definitions_received()` - Called after successfully fetching
       new definitions from the API. Store the data in your external cache
       and release any locks.

    4. `shutdown()` - Called when the PostHog client shuts down. Release any
       distributed locks and clean up resources.

    Error Handling:
        All methods are wrapped in try/except. Errors will be logged but will
        never break flag evaluation. On error:
        - `should_fetch_flag_definitions()` errors default to fetching (fail-safe)
        - `get_flag_definitions()` errors fall back to API fetch
        - `on_flag_definitions_received()` errors are logged but flags remain in memory
        - `shutdown()` errors are logged but shutdown continues
    """

    def get_flag_definitions(self) -> Optional[FlagDefinitionCacheData]:
        """
        Retrieve cached flag definitions.

        Returns:
            Cached flag definitions if available and valid, None otherwise.
            Returning None will trigger a fetch from the API if this worker
            has no flags loaded yet.
        """
        ...

    def should_fetch_flag_definitions(self) -> bool:
        """
        Determine whether this instance should fetch new flag definitions.

        Use this for distributed lock coordination. Only one worker should
        return True to avoid thundering herd problems. A typical implementation
        uses a distributed lock (e.g., Redis SETNX) that expires after the
        poll interval.

        Returns:
            True if this instance should fetch from the API, False otherwise.
            When False, the client will call `get_flag_definitions()` to
            retrieve cached data instead.
        """
        ...

    def on_flag_definitions_received(self, data: FlagDefinitionCacheData) -> None:
        """
        Called after successfully receiving new flag definitions from PostHog.

        Use this to store the data in your external cache and release any
        distributed locks acquired in `should_fetch_flag_definitions()`.

        Args:
            data: The flag definitions to cache, containing flags,
                  group_type_mapping, and cohorts.
        """
        ...

    def shutdown(self) -> None:
        """
        Called when the PostHog client shuts down.

        Use this to release any distributed locks and clean up resources.
        This method is called even if `should_fetch_flag_definitions()`
        returned False, so implementations should handle the case where
        no lock was acquired.
        """
        ...
