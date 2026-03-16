from __future__ import annotations

import base64
import functools
import json
import logging
from collections.abc import Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    TypeVar,
    Union,
)

from typing_extensions import TypedDict

from langsmith import client as ls_client
from langsmith import run_helpers
from langsmith._internal._beta_decorator import warn_beta
from langsmith.schemas import InputTokenDetails, OutputTokenDetails, UsageMetadata

if TYPE_CHECKING:
    from google import genai  # type: ignore[import-untyped, attr-defined]

C = TypeVar("C", bound=Union["genai.Client", Any])
logger = logging.getLogger(__name__)


def _strip_none(d: dict) -> dict:
    """Remove None values from dictionary."""
    return {k: v for k, v in d.items() if v is not None}


def _convert_config_for_tracing(kwargs: dict) -> None:
    """Convert GenerateContentConfig to dict for LangSmith compatibility."""
    if "config" in kwargs and not isinstance(kwargs["config"], dict):
        kwargs["config"] = vars(kwargs["config"])


def _process_gemini_inputs(inputs: dict) -> dict:
    r"""Process Gemini inputs to normalize them for LangSmith tracing.

    Example:
        {"contents": "Hello", "model": "gemini-pro"}
        → {"messages": [{"role": "user", "content": "Hello"}], "model": "gemini-pro"}
        {"contents": [{"role": "user", "parts": [{"text": "What is AI?"}]}], "model": "gemini-pro"}
        → {"messages": [{"role": "user", "content": "What is AI?"}], "model": "gemini-pro"}
    """  # noqa: E501
    # If contents is not present or not in list format, return as-is
    contents = inputs.get("contents")
    if not contents:
        return inputs

    # Handle string input (simple case)
    if isinstance(contents, str):
        return {
            "messages": [{"role": "user", "content": contents}],
            "model": inputs.get("model"),
            **({k: v for k, v in inputs.items() if k not in ("contents", "model")}),
        }

    # Handle list of content objects (multimodal case)
    if isinstance(contents, list):
        # Check if it's a simple list of strings
        if all(isinstance(item, str) for item in contents):
            # Each string becomes a separate user message (matches Gemini's behavior)
            return {
                "messages": [{"role": "user", "content": item} for item in contents],
                "model": inputs.get("model"),
                **({k: v for k, v in inputs.items() if k not in ("contents", "model")}),
            }
        # Handle complex multimodal case
        messages = []
        for content in contents:
            if isinstance(content, dict):
                role = content.get("role", "user")
                parts = content.get("parts", [])

                # Extract text and other parts
                text_parts = []
                content_parts = []

                for part in parts:
                    if isinstance(part, dict):
                        # Handle text parts
                        if "text" in part and part["text"]:
                            text_parts.append(part["text"])
                            content_parts.append({"type": "text", "text": part["text"]})
                        # Handle inline data (images)
                        elif "inline_data" in part:
                            inline_data = part["inline_data"]
                            mime_type = inline_data.get("mime_type", "image/jpeg")
                            data = inline_data.get("data", b"")

                            # Convert bytes to base64 string if needed
                            if isinstance(data, bytes):
                                data_b64 = base64.b64encode(data).decode("utf-8")
                            else:
                                data_b64 = data  # Already a string

                            content_parts.append(
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:{mime_type};base64,{data_b64}",
                                        "detail": "high",
                                    },
                                }
                            )
                        # Handle function responses
                        elif "functionResponse" in part:
                            function_response = part["functionResponse"]
                            content_parts.append(
                                {
                                    "type": "function_response",
                                    "function_response": {
                                        "name": function_response.get("name"),
                                        "response": function_response.get(
                                            "response", {}
                                        ),
                                    },
                                }
                            )
                        # Handle function calls (for conversation history)
                        elif "function_call" in part or "functionCall" in part:
                            function_call = part.get("function_call") or part.get(
                                "functionCall"
                            )

                            if function_call is not None:
                                # Normalize to dict (FunctionCall is a Pydantic model)
                                if not isinstance(function_call, dict):
                                    function_call = function_call.to_dict()

                                content_parts.append(
                                    {
                                        "type": "function_call",
                                        "function_call": {
                                            "id": function_call.get("id"),
                                            "name": function_call.get("name"),
                                            "arguments": function_call.get("args", {}),
                                        },
                                    }
                                )
                    elif isinstance(part, str):
                        # Handle simple string parts
                        text_parts.append(part)
                        content_parts.append({"type": "text", "text": part})

                # If only text parts, use simple string format
                if content_parts and all(
                    p.get("type") == "text" for p in content_parts
                ):
                    message_content: Union[str, list[dict[str, Any]]] = "\n".join(
                        text_parts
                    )
                else:
                    message_content = content_parts if content_parts else ""

                messages.append({"role": role, "content": message_content})
        return {
            "messages": messages,
            "model": inputs.get("model"),
            **({k: v for k, v in inputs.items() if k not in ("contents", "model")}),
        }

    # Fallback: return original inputs
    return inputs


def _infer_invocation_params(kwargs: dict) -> dict:
    """Extract invocation parameters for tracing."""
    stripped = _strip_none(kwargs)
    config = stripped.get("config", {})

    # Handle both dict config and GenerateContentConfig object
    if hasattr(config, "temperature"):
        temperature = config.temperature
        max_tokens = getattr(config, "max_output_tokens", None)
        stop = getattr(config, "stop_sequences", None)
    else:
        temperature = config.get("temperature")
        max_tokens = config.get("max_output_tokens")
        stop = config.get("stop_sequences")

    return {
        "ls_provider": "google",
        "ls_model_type": "chat",
        "ls_model_name": stripped.get("model"),
        "ls_temperature": temperature,
        "ls_max_tokens": max_tokens,
        "ls_stop": stop,
    }


def _create_usage_metadata(gemini_usage_metadata: dict) -> UsageMetadata:
    """Convert Gemini usage metadata to LangSmith format."""
    prompt_token_count = gemini_usage_metadata.get("prompt_token_count") or 0
    candidates_token_count = gemini_usage_metadata.get("candidates_token_count") or 0
    cached_content_token_count = (
        gemini_usage_metadata.get("cached_content_token_count") or 0
    )
    thoughts_token_count = gemini_usage_metadata.get("thoughts_token_count") or 0
    total_token_count = (
        gemini_usage_metadata.get("total_token_count")
        or prompt_token_count + candidates_token_count
    )

    input_token_details: dict = {}
    if cached_content_token_count:
        input_token_details["cache_read"] = cached_content_token_count

    output_token_details: dict = {}
    if thoughts_token_count:
        output_token_details["reasoning"] = thoughts_token_count

    return UsageMetadata(
        input_tokens=prompt_token_count,
        output_tokens=candidates_token_count,
        total_tokens=total_token_count,
        input_token_details=InputTokenDetails(
            **{k: v for k, v in input_token_details.items() if v is not None}
        ),
        output_token_details=OutputTokenDetails(
            **{k: v for k, v in output_token_details.items() if v is not None}
        ),
    )


def _process_generate_content_response(response: Any) -> dict:
    """Process Gemini response for tracing."""
    try:
        # Convert response to dictionary
        if hasattr(response, "to_dict"):
            rdict = response.to_dict()
        elif hasattr(response, "model_dump"):
            rdict = response.model_dump()
        else:
            rdict = {"text": getattr(response, "text", str(response))}

        # Extract content from candidates if available
        content_result = ""
        content_parts = []
        finish_reason: Optional[str] = None
        if "candidates" in rdict and rdict["candidates"]:
            candidate = rdict["candidates"][0]
            if "content" in candidate:
                content = candidate["content"]
                if "parts" in content and content["parts"]:
                    for part in content["parts"]:
                        # Handle text parts
                        if "text" in part and part["text"]:
                            content_result += part["text"]
                            content_parts.append({"type": "text", "text": part["text"]})
                        # Handle inline data (images) in response
                        elif "inline_data" in part and part["inline_data"] is not None:
                            inline_data = part["inline_data"]
                            mime_type = inline_data.get("mime_type", "image/jpeg")
                            data = inline_data.get("data", b"")

                            # Convert bytes to base64 string if needed
                            if isinstance(data, bytes):
                                data_b64 = base64.b64encode(data).decode("utf-8")
                            else:
                                data_b64 = data  # Already a string

                            content_parts.append(
                                {
                                    "type": "image_url",
                                    "image_url": {
                                        "url": f"data:{mime_type};base64,{data_b64}",
                                        "detail": "high",
                                    },
                                }
                            )
                        # Handle function calls in response
                        elif "function_call" in part or "functionCall" in part:
                            function_call = part.get("function_call") or part.get(
                                "functionCall"
                            )

                            if function_call is not None:
                                # Normalize to dict (FunctionCall is a Pydantic model)
                                if not isinstance(function_call, dict):
                                    function_call = function_call.to_dict()

                                content_parts.append(
                                    {
                                        "type": "function_call",
                                        "function_call": {
                                            "id": function_call.get("id"),
                                            "name": function_call.get("name"),
                                            "arguments": function_call.get("args", {}),
                                        },
                                    }
                                )
                if "finish_reason" in candidate and candidate["finish_reason"]:
                    finish_reason = candidate["finish_reason"]
        elif "text" in rdict:
            content_result = rdict["text"]
            content_parts.append({"type": "text", "text": content_result})

        # Build chat-like response format - use OpenAI-compatible format for tool calls
        tool_calls = [p for p in content_parts if p.get("type") == "function_call"]
        if tool_calls:
            # OpenAI-compatible format for LangSmith UI
            result = {
                "content": content_result or None,
                "role": "assistant",
                "finish_reason": finish_reason,
                "tool_calls": [
                    {
                        "id": tc["function_call"].get("id") or f"call_{i}",
                        "type": "function",
                        "index": i,
                        "function": {
                            "name": tc["function_call"]["name"],
                            "arguments": json.dumps(tc["function_call"]["arguments"]),
                        },
                    }
                    for i, tc in enumerate(tool_calls)
                ],
            }
        elif len(content_parts) > 1 or (
            content_parts and content_parts[0]["type"] != "text"
        ):
            # Use structured format for mixed non-tool content
            result = {
                "content": content_parts,
                "role": "assistant",
                "finish_reason": finish_reason,
            }
        else:
            # Use simple string format for text-only responses
            result = {
                "content": content_result,
                "role": "assistant",
                "finish_reason": finish_reason,
            }

        # Extract and convert usage metadata
        usage_metadata = rdict.get("usage_metadata")
        usage_dict: UsageMetadata = UsageMetadata(
            input_tokens=0, output_tokens=0, total_tokens=0
        )
        if usage_metadata:
            usage_dict = _create_usage_metadata(usage_metadata)
            # Add usage_metadata to both run.extra AND outputs
            current_run = run_helpers.get_current_run_tree()
            if current_run:
                try:
                    meta = current_run.extra.setdefault("metadata", {}).setdefault(
                        "usage_metadata", {}
                    )
                    meta.update(usage_dict)
                    current_run.patch()
                except Exception as e:
                    logger.warning(f"Failed to update usage metadata: {e}")

        # Return in a format that avoids stringification by LangSmith
        if result.get("tool_calls"):
            # For responses with tool calls, return structured format
            return {
                "content": result["content"],
                "role": "assistant",
                "finish_reason": finish_reason,
                "tool_calls": result["tool_calls"],
                "usage_metadata": usage_dict,
            }
        else:
            # For simple text responses, return minimal structure with usage metadata
            if isinstance(result["content"], str):
                return {
                    "content": result["content"],
                    "role": "assistant",
                    "finish_reason": finish_reason,
                    "usage_metadata": usage_dict,
                }
            else:
                # For multimodal content, return structured format with usage metadata
                return {
                    "content": result["content"],
                    "role": "assistant",
                    "finish_reason": finish_reason,
                    "usage_metadata": usage_dict,
                }
    except Exception as e:
        logger.debug(f"Error processing Gemini response: {e}")
        return {"output": response}


def _reduce_generate_content_chunks(all_chunks: list) -> dict:
    """Reduce streaming chunks into a single response."""
    if not all_chunks:
        return {
            "content": "",
            "usage_metadata": UsageMetadata(
                input_tokens=0, output_tokens=0, total_tokens=0
            ),
        }

    # Accumulate text from all chunks
    full_text = ""
    last_chunk = None

    for chunk in all_chunks:
        try:
            if hasattr(chunk, "text") and chunk.text:
                full_text += chunk.text
            last_chunk = chunk
        except Exception as e:
            logger.debug(f"Error processing chunk: {e}")

    # Extract usage metadata from the last chunk
    usage_metadata: UsageMetadata = UsageMetadata(
        input_tokens=0, output_tokens=0, total_tokens=0
    )
    if last_chunk:
        try:
            if hasattr(last_chunk, "usage_metadata") and last_chunk.usage_metadata:
                if hasattr(last_chunk.usage_metadata, "to_dict"):
                    usage_dict = last_chunk.usage_metadata.to_dict()
                elif hasattr(last_chunk.usage_metadata, "model_dump"):
                    usage_dict = last_chunk.usage_metadata.model_dump()
                else:
                    usage_dict = {
                        "prompt_token_count": getattr(
                            last_chunk.usage_metadata, "prompt_token_count", 0
                        ),
                        "candidates_token_count": getattr(
                            last_chunk.usage_metadata, "candidates_token_count", 0
                        ),
                        "cached_content_token_count": getattr(
                            last_chunk.usage_metadata, "cached_content_token_count", 0
                        ),
                        "thoughts_token_count": getattr(
                            last_chunk.usage_metadata, "thoughts_token_count", 0
                        ),
                        "total_token_count": getattr(
                            last_chunk.usage_metadata, "total_token_count", 0
                        ),
                    }
                # Add usage_metadata to both run.extra AND outputs
                usage_metadata = _create_usage_metadata(usage_dict)
                current_run = run_helpers.get_current_run_tree()
                if current_run:
                    try:
                        meta = current_run.extra.setdefault("metadata", {}).setdefault(
                            "usage_metadata", {}
                        )
                        meta.update(usage_metadata)
                        current_run.patch()
                    except Exception as e:
                        logger.warning(f"Failed to update usage metadata: {e}")
        except Exception as e:
            logger.debug(f"Error extracting metadata from last chunk: {e}")

    # Return minimal structure with usage_metadata in outputs
    return {
        "content": full_text,
        "usage_metadata": usage_metadata,
    }


def _get_wrapper(
    original_generate: Callable,
    name: str,
    tracing_extra: Optional[TracingExtra] = None,
    is_streaming: bool = False,
) -> Callable:
    """Create a wrapper for Gemini's generate_content methods."""
    textra = tracing_extra or {}

    @functools.wraps(original_generate)
    def generate(*args, **kwargs):
        # Handle config object before tracing setup
        _convert_config_for_tracing(kwargs)

        decorator = run_helpers.traceable(
            name=name,
            run_type="llm",
            reduce_fn=_reduce_generate_content_chunks if is_streaming else None,
            process_inputs=_process_gemini_inputs,
            process_outputs=(
                _process_generate_content_response if not is_streaming else None
            ),
            _invocation_params_fn=_infer_invocation_params,
            **textra,
        )

        return decorator(original_generate)(*args, **kwargs)

    @functools.wraps(original_generate)
    async def agenerate(*args, **kwargs):
        # Handle config object before tracing setup
        _convert_config_for_tracing(kwargs)

        decorator = run_helpers.traceable(
            name=name,
            run_type="llm",
            reduce_fn=_reduce_generate_content_chunks if is_streaming else None,
            process_inputs=_process_gemini_inputs,
            process_outputs=(
                _process_generate_content_response if not is_streaming else None
            ),
            _invocation_params_fn=_infer_invocation_params,
            **textra,
        )

        return await decorator(original_generate)(*args, **kwargs)

    return agenerate if run_helpers.is_async(original_generate) else generate


class TracingExtra(TypedDict, total=False):
    metadata: Optional[Mapping[str, Any]]
    tags: Optional[list[str]]
    client: Optional[ls_client.Client]


@warn_beta
def wrap_gemini(
    client: C,
    *,
    tracing_extra: Optional[TracingExtra] = None,
    chat_name: str = "ChatGoogleGenerativeAI",
) -> C:
    """Patch the Google Gen AI client to make it traceable.

    .. warning::
        **BETA**: This wrapper is in beta.

    Supports:
        - generate_content() and generate_content_stream() methods
        - Sync and async clients
        - Streaming and non-streaming responses
        - Tool/function calling with proper UI rendering
        - Multimodal inputs (text + images)
        - Image generation with inline_data support
        - Token usage tracking including reasoning tokens

    Args:
        client (genai.Client): The Google Gen AI client to patch.
        tracing_extra (Optional[TracingExtra], optional): Extra tracing information.
            Defaults to None.
        chat_name (str, optional): The run name for the chat endpoint.
            Defaults to "ChatGoogleGenerativeAI".

    Returns:
        genai.Client: The patched client.

    Examples:

        .. code-block:: python

            from google import genai
            from google.genai import types
            from langsmith import wrappers

            # Use Google Gen AI client same as you normally would.
            client = wrappers.wrap_gemini(genai.Client(api_key="your-api-key"))

            # Basic text generation:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents="Why is the sky blue?",
            )
            print(response.text)

            # Streaming:
            for chunk in client.models.generate_content_stream(
                model="gemini-2.5-flash",
                contents="Tell me a story",
            ):
                print(chunk.text, end="")

            # Tool/Function calling:
            schedule_meeting_function = {
                "name": "schedule_meeting",
                "description": "Schedules a meeting with specified attendees.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "attendees": {"type": "array", "items": {"type": "string"}},
                        "date": {"type": "string"},
                        "time": {"type": "string"},
                        "topic": {"type": "string"},
                    },
                    "required": ["attendees", "date", "time", "topic"],
                },
            }

            tools = types.Tool(function_declarations=[schedule_meeting_function])
            config = types.GenerateContentConfig(tools=[tools])

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents="Schedule a meeting with Bob and Alice tomorrow at 2 PM.",
                config=config,
            )

            # Image generation:
            response = client.models.generate_content(
                model="gemini-2.5-flash-image",
                contents=["Create a picture of a futuristic city"],
            )

            # Save generated image
            from io import BytesIO
            from PIL import Image

            for part in response.candidates[0].content.parts:
                if part.inline_data is not None:
                    image = Image.open(BytesIO(part.inline_data.data))
                    image.save("generated_image.png")

    .. versionadded:: 0.4.33
        Initial beta release of Google Gemini wrapper.

    """
    tracing_extra = tracing_extra or {}

    # Check if already wrapped to prevent double-wrapping
    if (
        hasattr(client, "models")
        and hasattr(client.models, "generate_content")
        and hasattr(client.models.generate_content, "__wrapped__")
    ):
        raise ValueError(
            "This Google Gen AI client has already been wrapped. "
            "Wrapping a client multiple times is not supported."
        )

    # Wrap synchronous methods
    if hasattr(client, "models") and hasattr(client.models, "generate_content"):
        client.models.generate_content = _get_wrapper(  # type: ignore[method-assign]
            client.models.generate_content,
            chat_name,
            tracing_extra=tracing_extra,
            is_streaming=False,
        )

    if hasattr(client, "models") and hasattr(client.models, "generate_content_stream"):
        client.models.generate_content_stream = _get_wrapper(  # type: ignore[method-assign]
            client.models.generate_content_stream,
            chat_name,
            tracing_extra=tracing_extra,
            is_streaming=True,
        )

    # Wrap async methods (aio namespace)
    if (
        hasattr(client, "aio")
        and hasattr(client.aio, "models")
        and hasattr(client.aio.models, "generate_content")
    ):
        client.aio.models.generate_content = _get_wrapper(  # type: ignore[method-assign]
            client.aio.models.generate_content,
            chat_name,
            tracing_extra=tracing_extra,
            is_streaming=False,
        )

    if (
        hasattr(client, "aio")
        and hasattr(client.aio, "models")
        and hasattr(client.aio.models, "generate_content_stream")
    ):
        client.aio.models.generate_content_stream = _get_wrapper(  # type: ignore[method-assign]
            client.aio.models.generate_content_stream,
            chat_name,
            tracing_extra=tracing_extra,
            is_streaming=True,
        )

    return client
