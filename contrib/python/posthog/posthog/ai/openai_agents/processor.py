import json
import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Union

from agents.tracing import Span, Trace
from agents.tracing.processor_interface import TracingProcessor
from agents.tracing.span_data import (
    AgentSpanData,
    CustomSpanData,
    FunctionSpanData,
    GenerationSpanData,
    GuardrailSpanData,
    HandoffSpanData,
    MCPListToolsSpanData,
    ResponseSpanData,
    SpeechGroupSpanData,
    SpeechSpanData,
    TranscriptionSpanData,
)

from posthog import setup
from posthog.client import Client

log = logging.getLogger("posthog")


def _ensure_serializable(obj: Any) -> Any:
    """Ensure an object is JSON-serializable, converting to str as fallback.

    Returns the original object if it's already serializable (dict, list, str,
    int, etc.), or str(obj) for non-serializable types so that downstream
    json.dumps() calls won't fail.
    """
    if obj is None:
        return None
    try:
        json.dumps(obj)
        return obj
    except (TypeError, ValueError):
        return str(obj)


def _parse_iso_timestamp(iso_str: Optional[str]) -> Optional[float]:
    """Parse ISO timestamp to Unix timestamp."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except (ValueError, AttributeError):
        return None


class PostHogTracingProcessor(TracingProcessor):
    """
    A tracing processor that sends OpenAI Agents SDK traces to PostHog.

    This processor implements the TracingProcessor interface from the OpenAI Agents SDK
    and maps agent traces, spans, and generations to PostHog's LLM analytics events.

    Example:
        ```python
        from agents import Agent, Runner
        from agents.tracing import add_trace_processor
        from posthog.ai.openai_agents import PostHogTracingProcessor

        # Create and register the processor
        processor = PostHogTracingProcessor(
            distinct_id="user@example.com",
            privacy_mode=False,
        )
        add_trace_processor(processor)

        # Run agents as normal - traces automatically sent to PostHog
        agent = Agent(name="Assistant", instructions="You are helpful.")
        result = Runner.run_sync(agent, "Hello!")
        ```
    """

    def __init__(
        self,
        client: Optional[Client] = None,
        distinct_id: Optional[Union[str, Callable[[Trace], Optional[str]]]] = None,
        privacy_mode: bool = False,
        groups: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the PostHog tracing processor.

        Args:
            client: Optional PostHog client instance. If not provided, uses the default client.
            distinct_id: Either a string distinct ID or a callable that takes a Trace
                and returns a distinct ID. If not provided, uses the trace_id.
            privacy_mode: If True, redacts input/output content from events.
            groups: Optional PostHog groups to associate with all events.
            properties: Optional additional properties to include with all events.
        """
        self._client = client or setup()
        self._distinct_id = distinct_id
        self._privacy_mode = privacy_mode
        self._groups = groups or {}
        self._properties = properties or {}

        # Track span start times for latency calculation
        self._span_start_times: Dict[str, float] = {}

        # Track trace metadata for associating with spans
        self._trace_metadata: Dict[str, Dict[str, Any]] = {}

        # Max entries to prevent unbounded growth if on_span_end/on_trace_end
        # is never called (e.g., due to an exception in the Agents SDK).
        self._max_tracked_entries = 10000

    def _get_distinct_id(self, trace: Optional[Trace]) -> Optional[str]:
        """Resolve the distinct ID for a trace.

        Returns the user-provided distinct ID (string or callable result),
        or None if no user-provided ID is available. Callers should treat
        None as a signal to use a fallback ID in personless mode.
        """
        if callable(self._distinct_id):
            if trace:
                result = self._distinct_id(trace)
                if result:
                    return str(result)
            return None
        elif self._distinct_id:
            return str(self._distinct_id)
        return None

    def _with_privacy_mode(self, value: Any) -> Any:
        """Apply privacy mode redaction if enabled."""
        if self._privacy_mode or (
            hasattr(self._client, "privacy_mode") and self._client.privacy_mode
        ):
            return None
        return value

    def _evict_stale_entries(self) -> None:
        """Evict oldest entries if dicts exceed max size to prevent unbounded growth."""
        if len(self._span_start_times) > self._max_tracked_entries:
            # Remove oldest entries by start time
            sorted_spans = sorted(self._span_start_times.items(), key=lambda x: x[1])
            for span_id, _ in sorted_spans[: len(sorted_spans) // 2]:
                del self._span_start_times[span_id]
            log.debug(
                "Evicted stale span start times (exceeded %d entries)",
                self._max_tracked_entries,
            )

        if len(self._trace_metadata) > self._max_tracked_entries:
            # Remove half the entries (oldest inserted via dict ordering in Python 3.7+)
            keys = list(self._trace_metadata.keys())
            for key in keys[: len(keys) // 2]:
                del self._trace_metadata[key]
            log.debug(
                "Evicted stale trace metadata (exceeded %d entries)",
                self._max_tracked_entries,
            )

    def _get_group_id(self, trace_id: str) -> Optional[str]:
        """Get the group_id for a trace from stored metadata."""
        if trace_id in self._trace_metadata:
            return self._trace_metadata[trace_id].get("group_id")
        return None

    def _capture_event(
        self,
        event: str,
        properties: Dict[str, Any],
        distinct_id: Optional[str] = None,
    ) -> None:
        """Capture an event to PostHog with error handling.

        Args:
            distinct_id: The resolved distinct ID. When the user didn't provide
                one, callers should pass ``user_distinct_id or fallback_id``
                (matching the langchain/openai pattern) and separately set
                ``$process_person_profile`` in properties.
        """
        try:
            if not hasattr(self._client, "capture") or not callable(
                self._client.capture
            ):
                return

            final_properties = {
                **properties,
                **self._properties,
            }

            self._client.capture(
                distinct_id=distinct_id or "unknown",
                event=event,
                properties=final_properties,
                groups=self._groups,
            )
        except Exception as e:
            log.debug(f"Failed to capture PostHog event: {e}")

    def on_trace_start(self, trace: Trace) -> None:
        """Called when a new trace begins. Stores metadata for spans; the $ai_trace event is emitted in on_trace_end."""
        try:
            self._evict_stale_entries()
            trace_id = trace.trace_id
            trace_name = trace.name
            group_id = getattr(trace, "group_id", None)
            metadata = getattr(trace, "metadata", None)

            distinct_id = self._get_distinct_id(trace)

            # Store trace metadata for later (used by spans and on_trace_end)
            self._trace_metadata[trace_id] = {
                "name": trace_name,
                "group_id": group_id,
                "metadata": metadata,
                "distinct_id": distinct_id,
                "start_time": time.time(),
            }
        except Exception as e:
            log.debug(f"Error in on_trace_start: {e}")

    def on_trace_end(self, trace: Trace) -> None:
        """Called when a trace completes. Emits the $ai_trace event with full metadata."""
        try:
            trace_id = trace.trace_id

            # Pop stored metadata (also cleans up)
            trace_info = self._trace_metadata.pop(trace_id, {})
            trace_name = trace_info.get("name") or trace.name
            group_id = trace_info.get("group_id") or getattr(trace, "group_id", None)
            metadata = trace_info.get("metadata") or getattr(trace, "metadata", None)
            distinct_id = trace_info.get("distinct_id") or self._get_distinct_id(trace)

            # Calculate trace-level latency
            start_time = trace_info.get("start_time")
            latency = (time.time() - start_time) if start_time else None

            properties = {
                "$ai_trace_id": trace_id,
                "$ai_trace_name": trace_name,
                "$ai_provider": "openai",
                "$ai_framework": "openai-agents",
            }

            if latency is not None:
                properties["$ai_latency"] = latency

            # Include group_id for linking related traces (e.g., conversation threads)
            if group_id:
                properties["$ai_group_id"] = group_id

            # Include trace metadata if present
            if metadata:
                properties["$ai_trace_metadata"] = _ensure_serializable(metadata)

            if distinct_id is None:
                properties["$process_person_profile"] = False

            self._capture_event(
                event="$ai_trace",
                distinct_id=distinct_id or trace_id,
                properties=properties,
            )
        except Exception as e:
            log.debug(f"Error in on_trace_end: {e}")

    def on_span_start(self, span: Span[Any]) -> None:
        """Called when a new span begins."""
        try:
            self._evict_stale_entries()
            span_id = span.span_id
            self._span_start_times[span_id] = time.time()
        except Exception as e:
            log.debug(f"Error in on_span_start: {e}")

    def on_span_end(self, span: Span[Any]) -> None:
        """Called when a span completes."""
        try:
            span_id = span.span_id
            trace_id = span.trace_id
            parent_id = span.parent_id
            span_data = span.span_data

            # Calculate latency
            start_time = self._span_start_times.pop(span_id, None)
            if start_time:
                latency = time.time() - start_time
            else:
                # Fall back to parsing timestamps
                started = _parse_iso_timestamp(span.started_at)
                ended = _parse_iso_timestamp(span.ended_at)
                latency = (ended - started) if (started and ended) else 0

            # Get user-provided distinct ID from trace metadata (resolved at trace start).
            # None means no user-provided ID — use trace_id as fallback in personless mode,
            # matching the langchain/openai pattern: `distinct_id or trace_id`.
            trace_info = self._trace_metadata.get(trace_id, {})
            distinct_id = trace_info.get("distinct_id") or self._get_distinct_id(None)

            # Get group_id from trace metadata for linking
            group_id = self._get_group_id(trace_id)

            # Get error info if present
            error_info = span.error
            error_properties = {}
            if error_info:
                if isinstance(error_info, dict):
                    error_message = error_info.get("message", str(error_info))
                    error_type_raw = error_info.get("type", "")
                else:
                    error_message = str(error_info)
                    error_type_raw = ""

                # Categorize error type for cross-provider filtering/alerting
                error_type = "unknown"
                if (
                    "ModelBehaviorError" in error_type_raw
                    or "ModelBehaviorError" in error_message
                ):
                    error_type = "model_behavior_error"
                elif "UserError" in error_type_raw or "UserError" in error_message:
                    error_type = "user_error"
                elif (
                    "InputGuardrailTripwireTriggered" in error_type_raw
                    or "InputGuardrailTripwireTriggered" in error_message
                ):
                    error_type = "input_guardrail_triggered"
                elif (
                    "OutputGuardrailTripwireTriggered" in error_type_raw
                    or "OutputGuardrailTripwireTriggered" in error_message
                ):
                    error_type = "output_guardrail_triggered"
                elif (
                    "MaxTurnsExceeded" in error_type_raw
                    or "MaxTurnsExceeded" in error_message
                ):
                    error_type = "max_turns_exceeded"

                error_properties = {
                    "$ai_is_error": True,
                    "$ai_error": error_message,
                    "$ai_error_type": error_type,
                }

            # Personless mode: no user-provided distinct_id, fallback to trace_id
            if distinct_id is None:
                error_properties["$process_person_profile"] = False
                distinct_id = trace_id

            # Dispatch based on span data type
            if isinstance(span_data, GenerationSpanData):
                self._handle_generation_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, FunctionSpanData):
                self._handle_function_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, AgentSpanData):
                self._handle_agent_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, HandoffSpanData):
                self._handle_handoff_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, GuardrailSpanData):
                self._handle_guardrail_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, ResponseSpanData):
                self._handle_response_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, CustomSpanData):
                self._handle_custom_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(
                span_data, (TranscriptionSpanData, SpeechSpanData, SpeechGroupSpanData)
            ):
                self._handle_audio_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            elif isinstance(span_data, MCPListToolsSpanData):
                self._handle_mcp_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )
            else:
                # Unknown span type - capture as generic span
                self._handle_generic_span(
                    span_data,
                    trace_id,
                    span_id,
                    parent_id,
                    latency,
                    distinct_id,
                    group_id,
                    error_properties,
                )

        except Exception as e:
            log.debug(f"Error in on_span_end: {e}")

    def _base_properties(
        self,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Build the base properties dict shared by all span handlers."""
        properties = {
            "$ai_trace_id": trace_id,
            "$ai_span_id": span_id,
            "$ai_parent_id": parent_id,
            "$ai_provider": "openai",
            "$ai_framework": "openai-agents",
            "$ai_latency": latency,
            **error_properties,
        }
        if group_id:
            properties["$ai_group_id"] = group_id
        return properties

    def _handle_generation_span(
        self,
        span_data: GenerationSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle LLM generation spans - maps to $ai_generation event."""
        # Extract token usage
        usage = span_data.usage or {}
        input_tokens = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
        output_tokens = (
            usage.get("output_tokens") or usage.get("completion_tokens") or 0
        )

        # Extract model config parameters
        model_config = span_data.model_config or {}
        model_params = {}
        for param in [
            "temperature",
            "max_tokens",
            "top_p",
            "frequency_penalty",
            "presence_penalty",
        ]:
            if param in model_config:
                model_params[param] = model_config[param]

        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_model": span_data.model,
            "$ai_model_parameters": model_params if model_params else None,
            "$ai_input": self._with_privacy_mode(_ensure_serializable(span_data.input)),
            "$ai_output_choices": self._with_privacy_mode(
                _ensure_serializable(span_data.output)
            ),
            "$ai_input_tokens": input_tokens,
            "$ai_output_tokens": output_tokens,
            "$ai_total_tokens": (input_tokens or 0) + (output_tokens or 0),
        }

        # Add optional token fields if present
        if usage.get("reasoning_tokens"):
            properties["$ai_reasoning_tokens"] = usage["reasoning_tokens"]
        if usage.get("cache_read_input_tokens"):
            properties["$ai_cache_read_input_tokens"] = usage["cache_read_input_tokens"]
        if usage.get("cache_creation_input_tokens"):
            properties["$ai_cache_creation_input_tokens"] = usage[
                "cache_creation_input_tokens"
            ]

        self._capture_event("$ai_generation", properties, distinct_id)

    def _handle_function_span(
        self,
        span_data: FunctionSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle function/tool call spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_data.name,
            "$ai_span_type": "tool",
            "$ai_input_state": self._with_privacy_mode(
                _ensure_serializable(span_data.input)
            ),
            "$ai_output_state": self._with_privacy_mode(
                _ensure_serializable(span_data.output)
            ),
        }

        if span_data.mcp_data:
            properties["$ai_mcp_data"] = _ensure_serializable(span_data.mcp_data)

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_agent_span(
        self,
        span_data: AgentSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle agent execution spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_data.name,
            "$ai_span_type": "agent",
        }

        if span_data.handoffs:
            properties["$ai_agent_handoffs"] = span_data.handoffs
        if span_data.tools:
            properties["$ai_agent_tools"] = span_data.tools
        if span_data.output_type:
            properties["$ai_agent_output_type"] = span_data.output_type

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_handoff_span(
        self,
        span_data: HandoffSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle agent handoff spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": f"{span_data.from_agent} -> {span_data.to_agent}",
            "$ai_span_type": "handoff",
            "$ai_handoff_from_agent": span_data.from_agent,
            "$ai_handoff_to_agent": span_data.to_agent,
        }

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_guardrail_span(
        self,
        span_data: GuardrailSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle guardrail execution spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_data.name,
            "$ai_span_type": "guardrail",
            "$ai_guardrail_triggered": span_data.triggered,
        }

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_response_span(
        self,
        span_data: ResponseSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle OpenAI Response API spans - maps to $ai_generation event."""
        response = span_data.response
        response_id = response.id if response else None

        # Try to extract usage from response
        usage = getattr(response, "usage", None) if response else None
        input_tokens = 0
        output_tokens = 0
        if usage:
            input_tokens = getattr(usage, "input_tokens", 0) or 0
            output_tokens = getattr(usage, "output_tokens", 0) or 0

        # Try to extract model from response
        model = getattr(response, "model", None) if response else None

        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_model": model,
            "$ai_response_id": response_id,
            "$ai_input": self._with_privacy_mode(_ensure_serializable(span_data.input)),
            "$ai_input_tokens": input_tokens,
            "$ai_output_tokens": output_tokens,
            "$ai_total_tokens": input_tokens + output_tokens,
        }

        # Extract output content from response
        if response:
            output_items = getattr(response, "output", None)
            if output_items:
                properties["$ai_output_choices"] = self._with_privacy_mode(
                    _ensure_serializable(output_items)
                )

        self._capture_event("$ai_generation", properties, distinct_id)

    def _handle_custom_span(
        self,
        span_data: CustomSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle custom user-defined spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_data.name,
            "$ai_span_type": "custom",
            "$ai_custom_data": self._with_privacy_mode(
                _ensure_serializable(span_data.data)
            ),
        }

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_audio_span(
        self,
        span_data: Union[TranscriptionSpanData, SpeechSpanData, SpeechGroupSpanData],
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle audio-related spans (transcription, speech) - maps to $ai_span event."""
        span_type = span_data.type  # "transcription", "speech", or "speech_group"

        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_type,
            "$ai_span_type": span_type,
        }

        # Add model info if available
        if hasattr(span_data, "model") and span_data.model:
            properties["$ai_model"] = span_data.model

        # Add model config if available (pass-through property)
        if hasattr(span_data, "model_config") and span_data.model_config:
            properties["model_config"] = _ensure_serializable(span_data.model_config)

        # Add time to first audio byte for speech spans (pass-through property)
        if hasattr(span_data, "first_content_at") and span_data.first_content_at:
            properties["first_content_at"] = span_data.first_content_at

        # Add audio format info (pass-through properties)
        if hasattr(span_data, "input_format"):
            properties["audio_input_format"] = span_data.input_format
        if hasattr(span_data, "output_format"):
            properties["audio_output_format"] = span_data.output_format

        # Add text input for TTS
        if (
            hasattr(span_data, "input")
            and span_data.input
            and isinstance(span_data.input, str)
        ):
            properties["$ai_input"] = self._with_privacy_mode(span_data.input)

        # Don't include audio data (base64) - just metadata
        if hasattr(span_data, "output") and isinstance(span_data.output, str):
            # For transcription, output is the text
            properties["$ai_output_state"] = self._with_privacy_mode(span_data.output)

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_mcp_span(
        self,
        span_data: MCPListToolsSpanData,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle MCP (Model Context Protocol) spans - maps to $ai_span event."""
        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": f"mcp:{span_data.server}",
            "$ai_span_type": "mcp_tools",
            "$ai_mcp_server": span_data.server,
            "$ai_mcp_tools": span_data.result,
        }

        self._capture_event("$ai_span", properties, distinct_id)

    def _handle_generic_span(
        self,
        span_data: Any,
        trace_id: str,
        span_id: str,
        parent_id: Optional[str],
        latency: float,
        distinct_id: str,
        group_id: Optional[str],
        error_properties: Dict[str, Any],
    ) -> None:
        """Handle unknown span types - maps to $ai_span event."""
        span_type = getattr(span_data, "type", "unknown")

        properties = {
            **self._base_properties(
                trace_id, span_id, parent_id, latency, group_id, error_properties
            ),
            "$ai_span_name": span_type,
            "$ai_span_type": span_type,
        }

        # Try to export span data
        if hasattr(span_data, "export"):
            try:
                exported = span_data.export()
                properties["$ai_span_data"] = _ensure_serializable(exported)
            except Exception:
                pass

        self._capture_event("$ai_span", properties, distinct_id)

    def shutdown(self) -> None:
        """Clean up resources when the application stops."""
        try:
            self._span_start_times.clear()
            self._trace_metadata.clear()

            # Flush the PostHog client if possible
            if hasattr(self._client, "flush") and callable(self._client.flush):
                self._client.flush()
        except Exception as e:
            log.debug(f"Error in shutdown: {e}")

    def force_flush(self) -> None:
        """Force immediate processing of any queued events."""
        try:
            if hasattr(self._client, "flush") and callable(self._client.flush):
                self._client.flush()
        except Exception as e:
            log.debug(f"Error in force_flush: {e}")
