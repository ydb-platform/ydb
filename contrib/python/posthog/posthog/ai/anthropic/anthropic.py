try:
    import anthropic
    from anthropic.resources import Messages
except ImportError:
    raise ModuleNotFoundError(
        "Please install the Anthropic SDK to use this feature: 'pip install anthropic'"
    )

import time
import uuid
from typing import Any, Dict, List, Optional

from posthog.ai.types import StreamingContentBlock, TokenUsage, ToolInProgress
from posthog.ai.utils import (
    call_llm_and_track_usage,
    merge_usage_stats,
)
from posthog.ai.anthropic.anthropic_converter import (
    extract_anthropic_usage_from_event,
    handle_anthropic_content_block_start,
    handle_anthropic_text_delta,
    handle_anthropic_tool_delta,
    finalize_anthropic_tool_input,
)
from posthog.ai.sanitization import sanitize_anthropic
from posthog.client import Client as PostHogClient
from posthog import setup


class Anthropic(anthropic.Anthropic):
    """
    A wrapper around the Anthropic SDK that automatically sends LLM usage events to PostHog.
    """

    _ph_client: PostHogClient

    def __init__(self, posthog_client: Optional[PostHogClient] = None, **kwargs):
        """
        Args:
            posthog_client: PostHog client for tracking usage
            **kwargs: Additional arguments passed to the Anthropic client
        """
        super().__init__(**kwargs)
        self._ph_client = posthog_client or setup()
        self.messages = WrappedMessages(self)


class WrappedMessages(Messages):
    _client: Anthropic

    def create(
        self,
        posthog_distinct_id: Optional[str] = None,
        posthog_trace_id: Optional[str] = None,
        posthog_properties: Optional[Dict[str, Any]] = None,
        posthog_privacy_mode: bool = False,
        posthog_groups: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Create a message using Anthropic's API while tracking usage in PostHog.

        Args:
            posthog_distinct_id: Optional ID to associate with the usage event
            posthog_trace_id: Optional trace UUID for linking events
            posthog_properties: Optional dictionary of extra properties to include in the event
            posthog_privacy_mode: Whether to redact sensitive information in tracking
            posthog_groups: Optional group analytics properties
            **kwargs: Arguments passed to Anthropic's messages.create
        """

        if posthog_trace_id is None:
            posthog_trace_id = str(uuid.uuid4())

        if kwargs.get("stream", False):
            return self._create_streaming(
                posthog_distinct_id,
                posthog_trace_id,
                posthog_properties,
                posthog_privacy_mode,
                posthog_groups,
                **kwargs,
            )

        return call_llm_and_track_usage(
            posthog_distinct_id,
            self._client._ph_client,
            "anthropic",
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            self._client.base_url,
            super().create,
            **kwargs,
        )

    def stream(
        self,
        posthog_distinct_id: Optional[str] = None,
        posthog_trace_id: Optional[str] = None,
        posthog_properties: Optional[Dict[str, Any]] = None,
        posthog_privacy_mode: bool = False,
        posthog_groups: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        if posthog_trace_id is None:
            posthog_trace_id = str(uuid.uuid4())

        return self._create_streaming(
            posthog_distinct_id,
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            **kwargs,
        )

    def _create_streaming(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        **kwargs: Any,
    ):
        start_time = time.time()
        usage_stats: TokenUsage = TokenUsage(input_tokens=0, output_tokens=0)
        accumulated_content = ""
        content_blocks: List[StreamingContentBlock] = []
        tools_in_progress: Dict[str, ToolInProgress] = {}
        current_text_block: Optional[StreamingContentBlock] = None
        response = super().create(**kwargs)

        def generator():
            nonlocal usage_stats
            nonlocal accumulated_content
            nonlocal content_blocks
            nonlocal tools_in_progress
            nonlocal current_text_block

            try:
                for event in response:
                    # Extract usage stats from event
                    event_usage = extract_anthropic_usage_from_event(event)
                    merge_usage_stats(usage_stats, event_usage)

                    # Handle content block start events
                    if hasattr(event, "type") and event.type == "content_block_start":
                        block, tool = handle_anthropic_content_block_start(event)

                        if block:
                            content_blocks.append(block)

                            if block.get("type") == "text":
                                current_text_block = block
                            else:
                                current_text_block = None

                        if tool:
                            tool_id = tool["block"].get("id")
                            if tool_id:
                                tools_in_progress[tool_id] = tool

                    # Handle text delta events
                    delta_text = handle_anthropic_text_delta(event, current_text_block)

                    if delta_text:
                        accumulated_content += delta_text

                    # Handle tool input delta events
                    handle_anthropic_tool_delta(
                        event, content_blocks, tools_in_progress
                    )

                    # Handle content block stop events
                    if hasattr(event, "type") and event.type == "content_block_stop":
                        current_text_block = None
                        finalize_anthropic_tool_input(
                            event, content_blocks, tools_in_progress
                        )

                    yield event

            finally:
                end_time = time.time()
                latency = end_time - start_time

                self._capture_streaming_event(
                    posthog_distinct_id,
                    posthog_trace_id,
                    posthog_properties,
                    posthog_privacy_mode,
                    posthog_groups,
                    kwargs,
                    usage_stats,
                    latency,
                    content_blocks,
                    accumulated_content,
                )

        return generator()

    def _capture_streaming_event(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
        usage_stats: TokenUsage,
        latency: float,
        content_blocks: List[StreamingContentBlock],
        accumulated_content: str,
    ):
        from posthog.ai.types import StreamingEventData
        from posthog.ai.anthropic.anthropic_converter import (
            format_anthropic_streaming_input,
            format_anthropic_streaming_output_complete,
        )
        from posthog.ai.utils import capture_streaming_event

        # Prepare standardized event data
        formatted_input = format_anthropic_streaming_input(kwargs)
        sanitized_input = sanitize_anthropic(formatted_input)

        event_data = StreamingEventData(
            provider="anthropic",
            model=kwargs.get("model", "unknown"),
            base_url=str(self._client.base_url),
            kwargs=kwargs,
            formatted_input=sanitized_input,
            formatted_output=format_anthropic_streaming_output_complete(
                content_blocks, accumulated_content
            ),
            usage_stats=usage_stats,
            latency=latency,
            distinct_id=posthog_distinct_id,
            trace_id=posthog_trace_id,
            properties=posthog_properties,
            privacy_mode=posthog_privacy_mode,
            groups=posthog_groups,
        )

        # Use the common capture function
        capture_streaming_event(self._client._ph_client, event_data)
