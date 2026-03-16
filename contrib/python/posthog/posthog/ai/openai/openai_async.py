import time
import uuid
from typing import Any, Dict, List, Optional

from posthog.ai.types import TokenUsage

try:
    import openai
except ImportError:
    raise ModuleNotFoundError(
        "Please install the OpenAI SDK to use this feature: 'pip install openai'"
    )

from posthog import setup
from posthog.ai.utils import (
    call_llm_and_track_usage_async,
    extract_available_tool_calls,
    get_model_params,
    merge_usage_stats,
    with_privacy_mode,
)
from posthog.ai.openai.openai_converter import (
    extract_openai_usage_from_chunk,
    extract_openai_content_from_chunk,
    extract_openai_tool_calls_from_chunk,
    accumulate_openai_tool_calls,
    format_openai_streaming_output,
)
from posthog.ai.sanitization import sanitize_openai, sanitize_openai_response
from posthog.client import Client as PostHogClient


class AsyncOpenAI(openai.AsyncOpenAI):
    """
    An async wrapper around the OpenAI SDK that automatically sends LLM usage events to PostHog.
    """

    _ph_client: PostHogClient

    def __init__(self, posthog_client: Optional[PostHogClient] = None, **kwargs):
        """
        Args:
            api_key: OpenAI API key.
            posthog_client: If provided, events will be captured via this client instead
                            of the global posthog.
            **openai_config: Any additional keyword args to set on openai (e.g. organization="xxx").
        """

        super().__init__(**kwargs)
        self._ph_client = posthog_client or setup()

        # Store original objects after parent initialization (only if they exist)
        self._original_chat = getattr(self, "chat", None)
        self._original_embeddings = getattr(self, "embeddings", None)
        self._original_beta = getattr(self, "beta", None)
        self._original_responses = getattr(self, "responses", None)

        # Replace with wrapped versions (only if originals exist)
        if self._original_chat is not None:
            self.chat = WrappedChat(self, self._original_chat)

        if self._original_embeddings is not None:
            self.embeddings = WrappedEmbeddings(self, self._original_embeddings)

        if self._original_beta is not None:
            self.beta = WrappedBeta(self, self._original_beta)

        if self._original_responses is not None:
            self.responses = WrappedResponses(self, self._original_responses)


class WrappedResponses:
    """Async wrapper for OpenAI responses that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_responses):
        self._client = client
        self._original = original_responses

    def __getattr__(self, name):
        """Fallback to original responses object for any methods we don't explicitly handle."""

        return getattr(self._original, name)

    async def create(
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

        if kwargs.get("stream", False):
            return await self._create_streaming(
                posthog_distinct_id,
                posthog_trace_id,
                posthog_properties,
                posthog_privacy_mode,
                posthog_groups,
                **kwargs,
            )

        return await call_llm_and_track_usage_async(
            posthog_distinct_id,
            self._client._ph_client,
            "openai",
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            self._client.base_url,
            self._original.create,
            **kwargs,
        )

    async def _create_streaming(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        **kwargs: Any,
    ):
        start_time = time.time()
        usage_stats: TokenUsage = TokenUsage()
        final_content = []
        model_from_response: Optional[str] = None
        response = await self._original.create(**kwargs)

        async def async_generator():
            nonlocal usage_stats
            nonlocal final_content  # noqa: F824
            nonlocal model_from_response

            try:
                async for chunk in response:
                    # Extract model from response object in chunk (for stored prompts)
                    if hasattr(chunk, "response") and chunk.response:
                        if model_from_response is None and hasattr(
                            chunk.response, "model"
                        ):
                            model_from_response = chunk.response.model

                    # Extract usage stats from chunk
                    chunk_usage = extract_openai_usage_from_chunk(chunk, "responses")

                    if chunk_usage:
                        merge_usage_stats(usage_stats, chunk_usage)

                    # Extract content from chunk
                    content = extract_openai_content_from_chunk(chunk, "responses")

                    if content is not None:
                        final_content.append(content)

                    yield chunk

            finally:
                end_time = time.time()
                latency = end_time - start_time
                output = final_content

                await self._capture_streaming_event(
                    posthog_distinct_id,
                    posthog_trace_id,
                    posthog_properties,
                    posthog_privacy_mode,
                    posthog_groups,
                    kwargs,
                    usage_stats,
                    latency,
                    output,
                    extract_available_tool_calls("openai", kwargs),
                    model_from_response,
                )

        return async_generator()

    async def _capture_streaming_event(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
        usage_stats: TokenUsage,
        latency: float,
        output: Any,
        available_tool_calls: Optional[List[Dict[str, Any]]] = None,
        model_from_response: Optional[str] = None,
    ):
        if posthog_trace_id is None:
            posthog_trace_id = str(uuid.uuid4())

        # Use model from kwargs, fallback to model from response
        model = kwargs.get("model") or model_from_response or "unknown"

        event_properties = {
            "$ai_provider": "openai",
            "$ai_model": model,
            "$ai_model_parameters": get_model_params(kwargs),
            "$ai_input": with_privacy_mode(
                self._client._ph_client,
                posthog_privacy_mode,
                sanitize_openai_response(kwargs.get("input")),
            ),
            "$ai_output_choices": with_privacy_mode(
                self._client._ph_client,
                posthog_privacy_mode,
                format_openai_streaming_output(output, "responses"),
            ),
            "$ai_http_status": 200,
            "$ai_input_tokens": usage_stats.get("input_tokens", 0),
            "$ai_output_tokens": usage_stats.get("output_tokens", 0),
            "$ai_cache_read_input_tokens": usage_stats.get(
                "cache_read_input_tokens", 0
            ),
            "$ai_reasoning_tokens": usage_stats.get("reasoning_tokens", 0),
            "$ai_latency": latency,
            "$ai_trace_id": posthog_trace_id,
            "$ai_base_url": str(self._client.base_url),
            **(posthog_properties or {}),
        }

        # Add web search count if present
        web_search_count = usage_stats.get("web_search_count")
        if (
            web_search_count is not None
            and isinstance(web_search_count, int)
            and web_search_count > 0
        ):
            event_properties["$ai_web_search_count"] = web_search_count

        if available_tool_calls:
            event_properties["$ai_tools"] = available_tool_calls

        if posthog_distinct_id is None:
            event_properties["$process_person_profile"] = False

        if hasattr(self._client._ph_client, "capture"):
            self._client._ph_client.capture(
                distinct_id=posthog_distinct_id or posthog_trace_id,
                event="$ai_generation",
                properties=event_properties,
                groups=posthog_groups,
            )

    async def parse(
        self,
        posthog_distinct_id: Optional[str] = None,
        posthog_trace_id: Optional[str] = None,
        posthog_properties: Optional[Dict[str, Any]] = None,
        posthog_privacy_mode: bool = False,
        posthog_groups: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Parse structured output using OpenAI's 'responses.parse' method, but also track usage in PostHog.

        Args:
            posthog_distinct_id: Optional ID to associate with the usage event.
            posthog_trace_id: Optional trace UUID for linking events.
            posthog_properties: Optional dictionary of extra properties to include in the event.
            posthog_privacy_mode: Whether to anonymize the input and output.
            posthog_groups: Optional dictionary of groups to associate with the event.
            **kwargs: Any additional parameters for the OpenAI Responses Parse API.

        Returns:
            The response from OpenAI's responses.parse call.
        """
        return await call_llm_and_track_usage_async(
            posthog_distinct_id,
            self._client._ph_client,
            "openai",
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            self._client.base_url,
            self._original.parse,
            **kwargs,
        )


class WrappedChat:
    """Async wrapper for OpenAI chat that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_chat):
        self._client = client
        self._original = original_chat

    def __getattr__(self, name):
        """Fallback to original chat object for any methods we don't explicitly handle."""
        return getattr(self._original, name)

    @property
    def completions(self):
        return WrappedCompletions(self._client, self._original.completions)


class WrappedCompletions:
    """Async wrapper for OpenAI chat completions that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_completions):
        self._client = client
        self._original = original_completions

    def __getattr__(self, name):
        """Fallback to original completions object for any methods we don't explicitly handle."""
        return getattr(self._original, name)

    async def create(
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

        # If streaming, handle streaming specifically
        if kwargs.get("stream", False):
            return await self._create_streaming(
                posthog_distinct_id,
                posthog_trace_id,
                posthog_properties,
                posthog_privacy_mode,
                posthog_groups,
                **kwargs,
            )

        response = await call_llm_and_track_usage_async(
            posthog_distinct_id,
            self._client._ph_client,
            "openai",
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            self._client.base_url,
            self._original.create,
            **kwargs,
        )
        return response

    async def _create_streaming(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        **kwargs: Any,
    ):
        start_time = time.time()
        usage_stats: TokenUsage = TokenUsage()
        accumulated_content = []
        accumulated_tool_calls: Dict[int, Dict[str, Any]] = {}
        model_from_response: Optional[str] = None

        if "stream_options" not in kwargs:
            kwargs["stream_options"] = {}
        kwargs["stream_options"]["include_usage"] = True
        response = await self._original.create(**kwargs)

        async def async_generator():
            nonlocal usage_stats
            nonlocal accumulated_content  # noqa: F824
            nonlocal accumulated_tool_calls
            nonlocal model_from_response

            try:
                async for chunk in response:
                    # Extract model from chunk (Chat Completions chunks have model field)
                    if model_from_response is None and hasattr(chunk, "model"):
                        model_from_response = chunk.model

                    # Extract usage stats from chunk
                    chunk_usage = extract_openai_usage_from_chunk(chunk, "chat")
                    if chunk_usage:
                        merge_usage_stats(usage_stats, chunk_usage)

                    # Extract content from chunk
                    content = extract_openai_content_from_chunk(chunk, "chat")
                    if content is not None:
                        accumulated_content.append(content)

                    # Extract and accumulate tool calls from chunk
                    chunk_tool_calls = extract_openai_tool_calls_from_chunk(chunk)
                    if chunk_tool_calls:
                        accumulate_openai_tool_calls(
                            accumulated_tool_calls, chunk_tool_calls
                        )

                    yield chunk

            finally:
                end_time = time.time()
                latency = end_time - start_time

                # Convert accumulated tool calls dict to list
                tool_calls_list = (
                    list(accumulated_tool_calls.values())
                    if accumulated_tool_calls
                    else None
                )

                await self._capture_streaming_event(
                    posthog_distinct_id,
                    posthog_trace_id,
                    posthog_properties,
                    posthog_privacy_mode,
                    posthog_groups,
                    kwargs,
                    usage_stats,
                    latency,
                    accumulated_content,
                    tool_calls_list,
                    extract_available_tool_calls("openai", kwargs),
                    model_from_response,
                )

        return async_generator()

    async def _capture_streaming_event(
        self,
        posthog_distinct_id: Optional[str],
        posthog_trace_id: Optional[str],
        posthog_properties: Optional[Dict[str, Any]],
        posthog_privacy_mode: bool,
        posthog_groups: Optional[Dict[str, Any]],
        kwargs: Dict[str, Any],
        usage_stats: TokenUsage,
        latency: float,
        output: Any,
        tool_calls: Optional[List[Dict[str, Any]]] = None,
        available_tool_calls: Optional[List[Dict[str, Any]]] = None,
        model_from_response: Optional[str] = None,
    ):
        if posthog_trace_id is None:
            posthog_trace_id = str(uuid.uuid4())

        # Use model from kwargs, fallback to model from response
        model = kwargs.get("model") or model_from_response or "unknown"

        event_properties = {
            "$ai_provider": "openai",
            "$ai_model": model,
            "$ai_model_parameters": get_model_params(kwargs),
            "$ai_input": with_privacy_mode(
                self._client._ph_client,
                posthog_privacy_mode,
                sanitize_openai(kwargs.get("messages")),
            ),
            "$ai_output_choices": with_privacy_mode(
                self._client._ph_client,
                posthog_privacy_mode,
                format_openai_streaming_output(output, "chat", tool_calls),
            ),
            "$ai_http_status": 200,
            "$ai_input_tokens": usage_stats.get("input_tokens", 0),
            "$ai_output_tokens": usage_stats.get("output_tokens", 0),
            "$ai_cache_read_input_tokens": usage_stats.get(
                "cache_read_input_tokens", 0
            ),
            "$ai_reasoning_tokens": usage_stats.get("reasoning_tokens", 0),
            "$ai_latency": latency,
            "$ai_trace_id": posthog_trace_id,
            "$ai_base_url": str(self._client.base_url),
            **(posthog_properties or {}),
        }

        # Add web search count if present
        web_search_count = usage_stats.get("web_search_count")

        if (
            web_search_count is not None
            and isinstance(web_search_count, int)
            and web_search_count > 0
        ):
            event_properties["$ai_web_search_count"] = web_search_count

        if available_tool_calls:
            event_properties["$ai_tools"] = available_tool_calls

        if posthog_distinct_id is None:
            event_properties["$process_person_profile"] = False

        if hasattr(self._client._ph_client, "capture"):
            self._client._ph_client.capture(
                distinct_id=posthog_distinct_id or posthog_trace_id,
                event="$ai_generation",
                properties=event_properties,
                groups=posthog_groups,
            )


class WrappedEmbeddings:
    """Async wrapper for OpenAI embeddings that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_embeddings):
        self._client = client
        self._original = original_embeddings

    def __getattr__(self, name):
        """Fallback to original embeddings object for any methods we don't explicitly handle."""

        return getattr(self._original, name)

    async def create(
        self,
        posthog_distinct_id: Optional[str] = None,
        posthog_trace_id: Optional[str] = None,
        posthog_properties: Optional[Dict[str, Any]] = None,
        posthog_privacy_mode: bool = False,
        posthog_groups: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        """
        Create an embedding using OpenAI's 'embeddings.create' method, but also track usage in PostHog.

        Args:
            posthog_distinct_id: Optional ID to associate with the usage event.
            posthog_trace_id: Optional trace UUID for linking events.
            posthog_properties: Optional dictionary of extra properties to include in the event.
            posthog_privacy_mode: Whether to anonymize the input and output.
            posthog_groups: Optional dictionary of groups to associate with the event.
            **kwargs: Any additional parameters for the OpenAI Embeddings API.

        Returns:
            The response from OpenAI's embeddings.create call.
        """

        if posthog_trace_id is None:
            posthog_trace_id = str(uuid.uuid4())

        start_time = time.time()
        response = await self._original.create(**kwargs)
        end_time = time.time()

        # Extract usage statistics if available
        usage_stats: TokenUsage = TokenUsage()

        if hasattr(response, "usage") and response.usage:
            usage_stats = TokenUsage(
                input_tokens=getattr(response.usage, "prompt_tokens", 0),
                output_tokens=getattr(response.usage, "completion_tokens", 0),
            )

        latency = end_time - start_time

        # Build the event properties
        event_properties = {
            "$ai_provider": "openai",
            "$ai_model": kwargs.get("model"),
            "$ai_input": with_privacy_mode(
                self._client._ph_client,
                posthog_privacy_mode,
                sanitize_openai_response(kwargs.get("input")),
            ),
            "$ai_http_status": 200,
            "$ai_input_tokens": usage_stats.get("input_tokens", 0),
            "$ai_latency": latency,
            "$ai_trace_id": posthog_trace_id,
            "$ai_base_url": str(self._client.base_url),
            **(posthog_properties or {}),
        }

        if posthog_distinct_id is None:
            event_properties["$process_person_profile"] = False

        # Send capture event for embeddings
        if hasattr(self._client._ph_client, "capture"):
            self._client._ph_client.capture(
                distinct_id=posthog_distinct_id or posthog_trace_id,
                event="$ai_embedding",
                properties=event_properties,
                groups=posthog_groups,
            )

        return response


class WrappedBeta:
    """Async wrapper for OpenAI beta features that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_beta):
        self._client = client
        self._original = original_beta

    def __getattr__(self, name):
        """Fallback to original beta object for any methods we don't explicitly handle."""

        return getattr(self._original, name)

    @property
    def chat(self):
        return WrappedBetaChat(self._client, self._original.chat)


class WrappedBetaChat:
    """Async wrapper for OpenAI beta chat that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_beta_chat):
        self._client = client
        self._original = original_beta_chat

    def __getattr__(self, name):
        """Fallback to original beta chat object for any methods we don't explicitly handle."""

        return getattr(self._original, name)

    @property
    def completions(self):
        return WrappedBetaCompletions(self._client, self._original.completions)


class WrappedBetaCompletions:
    """Async wrapper for OpenAI beta chat completions that tracks usage in PostHog."""

    def __init__(self, client: AsyncOpenAI, original_beta_completions):
        self._client = client
        self._original = original_beta_completions

    def __getattr__(self, name):
        """Fallback to original beta completions object for any methods we don't explicitly handle."""

        return getattr(self._original, name)

    async def parse(
        self,
        posthog_distinct_id: Optional[str] = None,
        posthog_trace_id: Optional[str] = None,
        posthog_properties: Optional[Dict[str, Any]] = None,
        posthog_privacy_mode: bool = False,
        posthog_groups: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        return await call_llm_and_track_usage_async(
            posthog_distinct_id,
            self._client._ph_client,
            "openai",
            posthog_trace_id,
            posthog_properties,
            posthog_privacy_mode,
            posthog_groups,
            self._client.base_url,
            self._original.parse,
            **kwargs,
        )
