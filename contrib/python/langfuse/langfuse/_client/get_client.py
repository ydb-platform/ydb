from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

from langfuse._client.client import Langfuse
from langfuse._client.resource_manager import LangfuseResourceManager
from langfuse.logger import langfuse_logger

# Context variable to track the current langfuse_public_key in execution context
_current_public_key: ContextVar[Optional[str]] = ContextVar(
    "langfuse_public_key", default=None
)


@contextmanager
def _set_current_public_key(public_key: Optional[str]) -> Iterator[None]:
    """Context manager to set and restore the current public key in execution context.

    Args:
        public_key: The public key to set in context. If None, context is not modified.

    Yields:
        None
    """
    if public_key is None:
        yield  # Don't modify context if no key provided
        return

    token = _current_public_key.set(public_key)
    try:
        yield
    finally:
        _current_public_key.reset(token)


def _create_client_from_instance(
    instance: "LangfuseResourceManager", public_key: Optional[str] = None
) -> Langfuse:
    """Create a Langfuse client from a resource manager instance with all settings preserved."""
    return Langfuse(
        public_key=public_key or instance.public_key,
        secret_key=instance.secret_key,
        base_url=instance.base_url,
        tracing_enabled=instance.tracing_enabled,
        environment=instance.environment,
        timeout=instance.timeout,
        flush_at=instance.flush_at,
        flush_interval=instance.flush_interval,
        release=instance.release,
        media_upload_thread_count=instance.media_upload_thread_count,
        sample_rate=instance.sample_rate,
        mask=instance.mask,
        blocked_instrumentation_scopes=instance.blocked_instrumentation_scopes,
        additional_headers=instance.additional_headers,
        tracer_provider=instance.tracer_provider,
        httpx_client=instance.httpx_client,
    )


def get_client(*, public_key: Optional[str] = None) -> Langfuse:
    """Get or create a Langfuse client instance.

    Returns an existing Langfuse client or creates a new one if none exists. In multi-project setups,
    providing a public_key is required. Multi-project support is experimental - see Langfuse docs.

    Behavior:
    - Single project: Returns existing client or creates new one
    - Multi-project: Requires public_key to return specific client
    - No public_key in multi-project: Returns disabled client to prevent data leakage

    The function uses a singleton pattern per public_key to conserve resources and maintain state.

    Args:
        public_key (Optional[str]): Project identifier
            - With key: Returns client for that project
            - Without key: Returns single client or disabled client if multiple exist

    Returns:
        Langfuse: Client instance in one of three states:
            1. Client for specified public_key
            2. Default client for single-project setup
            3. Disabled client when multiple projects exist without key

    Security:
        Disables tracing when multiple projects exist without explicit key to prevent
        cross-project data leakage. Multi-project setups are experimental.

    Example:
        ```python
        # Single project
        client = get_client()  # Default client

        # In multi-project usage:
        client_a = get_client(public_key="project_a_key")  # Returns project A's client
        client_b = get_client(public_key="project_b_key")  # Returns project B's client

        # Without specific key in multi-project setup:
        client = get_client()  # Returns disabled client for safety
        ```
    """
    with LangfuseResourceManager._lock:
        active_instances = LangfuseResourceManager._instances

        # If no explicit public_key provided, check execution context
        if not public_key:
            public_key = _current_public_key.get(None)

        if not public_key:
            if len(active_instances) == 0:
                # No clients initialized yet, create default instance
                return Langfuse()

            if len(active_instances) == 1:
                # Only one client exists, safe to use without specifying key
                instance = list(active_instances.values())[0]

                # Initialize with the credentials bound to the instance
                # This is important if the original instance was instantiated
                # via constructor arguments
                return _create_client_from_instance(instance)

            else:
                # Multiple clients exist but no key specified - disable tracing
                # to prevent cross-project data leakage
                langfuse_logger.warning(
                    "No 'langfuse_public_key' passed to decorated function, but multiple langfuse clients are instantiated in current process. Skipping tracing for this function to avoid cross-project leakage."
                )
                return Langfuse(
                    tracing_enabled=False, public_key="fake", secret_key="fake"
                )

        else:
            # Specific key provided, look up existing instance
            target_instance: Optional[LangfuseResourceManager] = active_instances.get(
                public_key, None
            )

            if target_instance is None:
                # No instance found with this key - client not initialized properly
                langfuse_logger.warning(
                    f"No Langfuse client with public key {public_key} has been initialized. Skipping tracing for decorated function."
                )
                return Langfuse(
                    tracing_enabled=False, public_key="fake", secret_key="fake"
                )

            # target_instance is guaranteed to be not None at this point
            return _create_client_from_instance(target_instance, public_key)
