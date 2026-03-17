from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

if TYPE_CHECKING:
    from agents.tracing import Trace

    from posthog.client import Client

try:
    import agents  # noqa: F401
except ImportError:
    raise ModuleNotFoundError(
        "Please install the OpenAI Agents SDK to use this feature: 'pip install openai-agents'"
    )

from posthog.ai.openai_agents.processor import PostHogTracingProcessor

__all__ = ["PostHogTracingProcessor", "instrument"]


def instrument(
    client: Optional[Client] = None,
    distinct_id: Optional[Union[str, Callable[[Trace], Optional[str]]]] = None,
    privacy_mode: bool = False,
    groups: Optional[Dict[str, Any]] = None,
    properties: Optional[Dict[str, Any]] = None,
) -> PostHogTracingProcessor:
    """
    One-liner to instrument OpenAI Agents SDK with PostHog tracing.

    This registers a PostHogTracingProcessor with the OpenAI Agents SDK,
    automatically capturing traces, spans, and LLM generations.

    Args:
        client: Optional PostHog client instance. If not provided, uses the default client.
        distinct_id: Optional distinct ID to associate with all traces.
            Can also be a callable that takes a trace and returns a distinct ID.
        privacy_mode: If True, redacts input/output content from events.
        groups: Optional PostHog groups to associate with events.
        properties: Optional additional properties to include with all events.

    Returns:
        PostHogTracingProcessor: The registered processor instance.

    Example:
        ```python
        from posthog.ai.openai_agents import instrument

        # Simple setup
        instrument(distinct_id="user@example.com")

        # With custom properties
        instrument(
            distinct_id="user@example.com",
            privacy_mode=True,
            properties={"environment": "production"}
        )

        # Now run agents as normal - traces automatically sent to PostHog
        from agents import Agent, Runner
        agent = Agent(name="Assistant", instructions="You are helpful.")
        result = Runner.run_sync(agent, "Hello!")
        ```
    """
    from agents.tracing import add_trace_processor

    processor = PostHogTracingProcessor(
        client=client,
        distinct_id=distinct_id,
        privacy_mode=privacy_mode,
        groups=groups,
        properties=properties,
    )
    add_trace_processor(processor)
    return processor
