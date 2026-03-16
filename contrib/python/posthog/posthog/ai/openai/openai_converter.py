"""
OpenAI-specific conversion utilities.

This module handles the conversion of OpenAI API responses and inputs
into standardized formats for PostHog tracking. It supports both
Chat Completions API and Responses API formats.
"""

from typing import Any, Dict, List, Optional

from posthog.ai.types import (
    FormattedContentItem,
    FormattedFunctionCall,
    FormattedImageContent,
    FormattedMessage,
    FormattedTextContent,
    TokenUsage,
)
from posthog.ai.utils import serialize_raw_usage


def format_openai_response(response: Any) -> List[FormattedMessage]:
    """
    Format an OpenAI response into standardized message format.

    Handles both Chat Completions API and Responses API formats.

    Args:
        response: The response object from OpenAI API

    Returns:
        List of formatted messages with role and content
    """

    output: List[FormattedMessage] = []

    if response is None:
        return output

    # Handle Chat Completions response format
    if hasattr(response, "choices"):
        content: List[FormattedContentItem] = []
        role = "assistant"

        for choice in response.choices:
            if hasattr(choice, "message") and choice.message:
                if choice.message.role:
                    role = choice.message.role

                if choice.message.content:
                    content.append(
                        {
                            "type": "text",
                            "text": choice.message.content,
                        }
                    )

                if hasattr(choice.message, "tool_calls") and choice.message.tool_calls:
                    for tool_call in choice.message.tool_calls:
                        content.append(
                            {
                                "type": "function",
                                "id": tool_call.id,
                                "function": {
                                    "name": tool_call.function.name,
                                    "arguments": tool_call.function.arguments,
                                },
                            }
                        )

                # Handle audio output (gpt-4o-audio-preview)
                if hasattr(choice.message, "audio") and choice.message.audio:
                    # Convert Pydantic model to dict to capture all fields from OpenAI
                    audio_dict = choice.message.audio.model_dump()
                    content.append({"type": "audio", **audio_dict})

        if content:
            output.append(
                {
                    "role": role,
                    "content": content,
                }
            )

    # Handle Responses API format
    if hasattr(response, "output"):
        content = []
        role = "assistant"

        for item in response.output:
            if item.type == "message":
                role = item.role

                if hasattr(item, "content") and isinstance(item.content, list):
                    for content_item in item.content:
                        if (
                            hasattr(content_item, "type")
                            and content_item.type == "output_text"
                            and hasattr(content_item, "text")
                        ):
                            content.append(
                                {
                                    "type": "text",
                                    "text": content_item.text,
                                }
                            )

                        elif hasattr(content_item, "text"):
                            content.append({"type": "text", "text": content_item.text})

                        elif (
                            hasattr(content_item, "type")
                            and content_item.type == "input_image"
                            and hasattr(content_item, "image_url")
                        ):
                            image_content: FormattedImageContent = {
                                "type": "image",
                                "image": content_item.image_url,
                            }
                            content.append(image_content)

                elif hasattr(item, "content"):
                    text_content = {"type": "text", "text": str(item.content)}
                    content.append(text_content)

            elif hasattr(item, "type") and item.type == "function_call":
                content.append(
                    {
                        "type": "function",
                        "id": getattr(item, "call_id", getattr(item, "id", "")),
                        "function": {
                            "name": item.name,
                            "arguments": getattr(item, "arguments", {}),
                        },
                    }
                )

        if content:
            output.append(
                {
                    "role": role,
                    "content": content,
                }
            )

    return output


def format_openai_input(
    messages: Optional[List[Dict[str, Any]]] = None, input_data: Optional[Any] = None
) -> List[FormattedMessage]:
    """
    Format OpenAI input messages.

    Handles both messages parameter (Chat Completions) and input parameter (Responses API).

    Args:
        messages: List of message dictionaries for Chat Completions API
        input_data: Input data for Responses API

    Returns:
        List of formatted messages
    """

    formatted_messages: List[FormattedMessage] = []

    # Handle Chat Completions API format
    if messages is not None:
        for msg in messages:
            formatted_messages.append(
                {
                    "role": msg.get("role", "user"),
                    "content": msg.get("content", ""),
                }
            )

    # Handle Responses API format
    if input_data is not None:
        if isinstance(input_data, list):
            for item in input_data:
                role = "user"
                content = ""

                if isinstance(item, dict):
                    role = item.get("role", "user")
                    content = item.get("content", "")

                elif isinstance(item, str):
                    content = item

                else:
                    content = str(item)

                formatted_messages.append({"role": role, "content": content})

        elif isinstance(input_data, str):
            formatted_messages.append({"role": "user", "content": input_data})

        else:
            formatted_messages.append({"role": "user", "content": str(input_data)})

    return formatted_messages


def extract_openai_tools(kwargs: Dict[str, Any]) -> Optional[Any]:
    """
    Extract tool definitions from OpenAI API kwargs.

    Args:
        kwargs: Keyword arguments passed to OpenAI API

    Returns:
        Tool definitions if present, None otherwise
    """

    # Check for tools parameter (newer API)
    if "tools" in kwargs:
        return kwargs["tools"]

    # Check for functions parameter (older API)
    if "functions" in kwargs:
        return kwargs["functions"]

    return None


def format_openai_streaming_content(
    accumulated_content: str, tool_calls: Optional[List[Dict[str, Any]]] = None
) -> List[FormattedContentItem]:
    """
    Format content from OpenAI streaming response.

    Used by streaming handlers to format accumulated content.

    Args:
        accumulated_content: Accumulated text content from streaming
        tool_calls: Optional list of tool calls accumulated during streaming

    Returns:
        List of formatted content items
    """
    formatted: List[FormattedContentItem] = []

    # Add text content if present
    if accumulated_content:
        text_content: FormattedTextContent = {
            "type": "text",
            "text": accumulated_content,
        }
        formatted.append(text_content)

    # Add tool calls if present
    if tool_calls:
        for tool_call in tool_calls:
            function_call: FormattedFunctionCall = {
                "type": "function",
                "id": tool_call.get("id"),
                "function": tool_call.get("function", {}),
            }
            formatted.append(function_call)

    return formatted


def extract_openai_web_search_count(response: Any) -> int:
    """
    Extract web search count from OpenAI response.

    Uses a two-tier detection strategy:
    1. Priority 1 (exact count): Check for output[].type == "web_search_call" (Responses API)
    2. Priority 2 (binary detection): Check for various web search indicators:
       - Root-level citations, search_results, or usage.search_context_size (Perplexity)
       - Annotations with type "url_citation" in choices/output (including delta for streaming)

    Args:
        response: The response from OpenAI API

    Returns:
        Number of web search requests (exact count or binary 1/0)
    """

    # Priority 1: Check for exact count in Responses API output
    if hasattr(response, "output"):
        web_search_count = 0

        for item in response.output:
            if hasattr(item, "type") and item.type == "web_search_call":
                web_search_count += 1

        web_search_count = max(0, web_search_count)

        if web_search_count > 0:
            return web_search_count

    # Priority 2: Binary detection (returns 1 or 0)

    # Check root-level indicators (Perplexity)
    if hasattr(response, "citations"):
        citations = getattr(response, "citations")

        if citations and len(citations) > 0:
            return 1

    if hasattr(response, "search_results"):
        search_results = getattr(response, "search_results")

        if search_results and len(search_results) > 0:
            return 1

    if hasattr(response, "usage") and hasattr(response.usage, "search_context_size"):
        if response.usage.search_context_size:
            return 1

    # Check for url_citation annotations in choices (Chat Completions)
    if hasattr(response, "choices"):
        for choice in response.choices:
            # Check message.annotations (non-streaming or final chunk)
            if hasattr(choice, "message") and hasattr(choice.message, "annotations"):
                annotations = choice.message.annotations

                if annotations:
                    for annotation in annotations:
                        # Support both dict and object formats
                        annotation_type = (
                            annotation.get("type")
                            if isinstance(annotation, dict)
                            else getattr(annotation, "type", None)
                        )

                        if annotation_type == "url_citation":
                            return 1

            # Check delta.annotations (streaming chunks)
            if hasattr(choice, "delta") and hasattr(choice.delta, "annotations"):
                annotations = choice.delta.annotations

                if annotations:
                    for annotation in annotations:
                        # Support both dict and object formats
                        annotation_type = (
                            annotation.get("type")
                            if isinstance(annotation, dict)
                            else getattr(annotation, "type", None)
                        )

                        if annotation_type == "url_citation":
                            return 1

    # Check for url_citation annotations in output (Responses API)
    if hasattr(response, "output"):
        for item in response.output:
            if hasattr(item, "content") and isinstance(item.content, list):
                for content_item in item.content:
                    if hasattr(content_item, "annotations"):
                        annotations = content_item.annotations

                        if annotations:
                            for annotation in annotations:
                                # Support both dict and object formats
                                annotation_type = (
                                    annotation.get("type")
                                    if isinstance(annotation, dict)
                                    else getattr(annotation, "type", None)
                                )

                                if annotation_type == "url_citation":
                                    return 1

    return 0


def extract_openai_usage_from_response(response: Any) -> TokenUsage:
    """
    Extract usage statistics from a full OpenAI response (non-streaming).
    Handles both Chat Completions and Responses API.

    Args:
        response: The complete response from OpenAI API

    Returns:
        TokenUsage with standardized usage statistics
    """
    if not hasattr(response, "usage"):
        return TokenUsage(input_tokens=0, output_tokens=0)

    cached_tokens = 0
    input_tokens = 0
    output_tokens = 0
    reasoning_tokens = 0

    # Responses API format
    if hasattr(response.usage, "input_tokens"):
        input_tokens = response.usage.input_tokens
    if hasattr(response.usage, "output_tokens"):
        output_tokens = response.usage.output_tokens
    if hasattr(response.usage, "input_tokens_details") and hasattr(
        response.usage.input_tokens_details, "cached_tokens"
    ):
        cached_tokens = response.usage.input_tokens_details.cached_tokens
    if hasattr(response.usage, "output_tokens_details") and hasattr(
        response.usage.output_tokens_details, "reasoning_tokens"
    ):
        reasoning_tokens = response.usage.output_tokens_details.reasoning_tokens

    # Chat Completions format
    if hasattr(response.usage, "prompt_tokens"):
        input_tokens = response.usage.prompt_tokens
    if hasattr(response.usage, "completion_tokens"):
        output_tokens = response.usage.completion_tokens
    if hasattr(response.usage, "prompt_tokens_details") and hasattr(
        response.usage.prompt_tokens_details, "cached_tokens"
    ):
        cached_tokens = response.usage.prompt_tokens_details.cached_tokens
    if hasattr(response.usage, "completion_tokens_details") and hasattr(
        response.usage.completion_tokens_details, "reasoning_tokens"
    ):
        reasoning_tokens = response.usage.completion_tokens_details.reasoning_tokens

    result = TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )

    if cached_tokens > 0:
        result["cache_read_input_tokens"] = cached_tokens
    if reasoning_tokens > 0:
        result["reasoning_tokens"] = reasoning_tokens

    web_search_count = extract_openai_web_search_count(response)
    if web_search_count > 0:
        result["web_search_count"] = web_search_count

    # Capture raw usage metadata for backend processing
    # Serialize to dict here in the converter (not in utils)
    serialized = serialize_raw_usage(response.usage)
    if serialized:
        result["raw_usage"] = serialized

    return result


def extract_openai_usage_from_chunk(
    chunk: Any, provider_type: str = "chat"
) -> TokenUsage:
    """
    Extract usage statistics from an OpenAI streaming chunk.

    Handles both Chat Completions and Responses API formats.

    Args:
        chunk: Streaming chunk from OpenAI API
        provider_type: Either "chat" or "responses" to handle different API formats

    Returns:
        Dictionary of usage statistics
    """

    usage: TokenUsage = TokenUsage()

    if provider_type == "chat":
        # Extract web search count from the chunk before checking for usage
        # Web search indicators (citations, annotations) can appear on any chunk,
        # not just those with usage data
        web_search_count = extract_openai_web_search_count(chunk)
        if web_search_count > 0:
            usage["web_search_count"] = web_search_count

        if not hasattr(chunk, "usage") or not chunk.usage:
            return usage

        # Chat Completions API uses prompt_tokens and completion_tokens
        # Standardize to input_tokens and output_tokens
        usage["input_tokens"] = getattr(chunk.usage, "prompt_tokens", 0)
        usage["output_tokens"] = getattr(chunk.usage, "completion_tokens", 0)

        # Handle cached tokens
        if hasattr(chunk.usage, "prompt_tokens_details") and hasattr(
            chunk.usage.prompt_tokens_details, "cached_tokens"
        ):
            usage["cache_read_input_tokens"] = (
                chunk.usage.prompt_tokens_details.cached_tokens
            )

        # Handle reasoning tokens
        if hasattr(chunk.usage, "completion_tokens_details") and hasattr(
            chunk.usage.completion_tokens_details, "reasoning_tokens"
        ):
            usage["reasoning_tokens"] = (
                chunk.usage.completion_tokens_details.reasoning_tokens
            )

        # Capture raw usage metadata for backend processing
        # Serialize to dict here in the converter (not in utils)
        serialized = serialize_raw_usage(chunk.usage)
        if serialized:
            usage["raw_usage"] = serialized

    elif provider_type == "responses":
        # For Responses API, usage is only in chunk.response.usage for completed events
        if hasattr(chunk, "type") and chunk.type == "response.completed":
            if (
                hasattr(chunk, "response")
                and hasattr(chunk.response, "usage")
                and chunk.response.usage
            ):
                response_usage = chunk.response.usage
                usage["input_tokens"] = getattr(response_usage, "input_tokens", 0)
                usage["output_tokens"] = getattr(response_usage, "output_tokens", 0)

                # Handle cached tokens
                if hasattr(response_usage, "input_tokens_details") and hasattr(
                    response_usage.input_tokens_details, "cached_tokens"
                ):
                    usage["cache_read_input_tokens"] = (
                        response_usage.input_tokens_details.cached_tokens
                    )

                # Handle reasoning tokens
                if hasattr(response_usage, "output_tokens_details") and hasattr(
                    response_usage.output_tokens_details, "reasoning_tokens"
                ):
                    usage["reasoning_tokens"] = (
                        response_usage.output_tokens_details.reasoning_tokens
                    )

                # Extract web search count from the complete response
                if hasattr(chunk, "response"):
                    web_search_count = extract_openai_web_search_count(chunk.response)
                    if web_search_count > 0:
                        usage["web_search_count"] = web_search_count

                # Capture raw usage metadata for backend processing
                # Serialize to dict here in the converter (not in utils)
                serialized = serialize_raw_usage(response_usage)
                if serialized:
                    usage["raw_usage"] = serialized

    return usage


def extract_openai_content_from_chunk(
    chunk: Any, provider_type: str = "chat"
) -> Optional[str]:
    """
    Extract content from an OpenAI streaming chunk.

    Handles both Chat Completions and Responses API formats.

    Args:
        chunk: Streaming chunk from OpenAI API
        provider_type: Either "chat" or "responses" to handle different API formats

    Returns:
        Text content if present, None otherwise
    """

    if provider_type == "chat":
        # Chat Completions API format
        if (
            hasattr(chunk, "choices")
            and chunk.choices
            and len(chunk.choices) > 0
            and chunk.choices[0].delta
            and chunk.choices[0].delta.content
        ):
            return chunk.choices[0].delta.content

    elif provider_type == "responses":
        # Responses API format
        if hasattr(chunk, "type") and chunk.type == "response.completed":
            if hasattr(chunk, "response") and chunk.response:
                res = chunk.response
                if res.output and len(res.output) > 0:
                    # Return the full output for responses
                    return res.output[0]

    return None


def extract_openai_tool_calls_from_chunk(chunk: Any) -> Optional[List[Dict[str, Any]]]:
    """
    Extract tool calls from an OpenAI streaming chunk.

    Args:
        chunk: Streaming chunk from OpenAI API

    Returns:
        List of tool call deltas if present, None otherwise
    """
    if (
        hasattr(chunk, "choices")
        and chunk.choices
        and len(chunk.choices) > 0
        and chunk.choices[0].delta
        and hasattr(chunk.choices[0].delta, "tool_calls")
        and chunk.choices[0].delta.tool_calls
    ):
        tool_calls = []
        for tool_call in chunk.choices[0].delta.tool_calls:
            tc_dict = {
                "index": getattr(tool_call, "index", None),
            }

            if hasattr(tool_call, "id") and tool_call.id:
                tc_dict["id"] = tool_call.id

            if hasattr(tool_call, "type") and tool_call.type:
                tc_dict["type"] = tool_call.type

            if hasattr(tool_call, "function") and tool_call.function:
                function_dict = {}
                if hasattr(tool_call.function, "name") and tool_call.function.name:
                    function_dict["name"] = tool_call.function.name
                if (
                    hasattr(tool_call.function, "arguments")
                    and tool_call.function.arguments
                ):
                    function_dict["arguments"] = tool_call.function.arguments
                tc_dict["function"] = function_dict

            tool_calls.append(tc_dict)
        return tool_calls

    return None


def accumulate_openai_tool_calls(
    accumulated_tool_calls: Dict[int, Dict[str, Any]],
    chunk_tool_calls: List[Dict[str, Any]],
) -> None:
    """
    Accumulate tool calls from streaming chunks.

    OpenAI sends tool calls incrementally:
    - First chunk has id, type, function.name and partial function.arguments
    - Subsequent chunks have more function.arguments

    Args:
        accumulated_tool_calls: Dictionary mapping index to accumulated tool call data
        chunk_tool_calls: List of tool call deltas from current chunk
    """
    for tool_call_delta in chunk_tool_calls:
        index = tool_call_delta.get("index")
        if index is None:
            continue

        # Initialize tool call if first time seeing this index
        if index not in accumulated_tool_calls:
            accumulated_tool_calls[index] = {
                "id": "",
                "type": "function",
                "function": {
                    "name": "",
                    "arguments": "",
                },
            }

        # Update with new data from delta
        tc = accumulated_tool_calls[index]

        if "id" in tool_call_delta and tool_call_delta["id"]:
            tc["id"] = tool_call_delta["id"]

        if "type" in tool_call_delta and tool_call_delta["type"]:
            tc["type"] = tool_call_delta["type"]

        if "function" in tool_call_delta:
            func_delta = tool_call_delta["function"]
            if "name" in func_delta and func_delta["name"]:
                tc["function"]["name"] = func_delta["name"]
            if "arguments" in func_delta and func_delta["arguments"]:
                # Arguments are sent incrementally, concatenate them
                tc["function"]["arguments"] += func_delta["arguments"]


def format_openai_streaming_output(
    accumulated_content: Any,
    provider_type: str = "chat",
    tool_calls: Optional[List[Dict[str, Any]]] = None,
) -> List[FormattedMessage]:
    """
    Format the final output from OpenAI streaming.

    Args:
        accumulated_content: Accumulated content from streaming (string for chat, list for responses)
        provider_type: Either "chat" or "responses" to handle different API formats
        tool_calls: Optional list of accumulated tool calls

    Returns:
        List of formatted messages
    """

    if provider_type == "chat":
        content_items: List[FormattedContentItem] = []

        # Add text content if present
        if isinstance(accumulated_content, str) and accumulated_content:
            content_items.append({"type": "text", "text": accumulated_content})
        elif isinstance(accumulated_content, list):
            # If it's a list of strings, join them
            text = "".join(str(item) for item in accumulated_content if item)
            if text:
                content_items.append({"type": "text", "text": text})

        # Add tool calls if present
        if tool_calls:
            for tool_call in tool_calls:
                if "function" in tool_call:
                    function_call: FormattedFunctionCall = {
                        "type": "function",
                        "id": tool_call.get("id", ""),
                        "function": tool_call["function"],
                    }
                    content_items.append(function_call)

        # Return formatted message with content
        if content_items:
            return [{"role": "assistant", "content": content_items}]
        else:
            # Empty response
            return [{"role": "assistant", "content": []}]

    elif provider_type == "responses":
        # Responses API: accumulated_content is a list of output items
        if isinstance(accumulated_content, list) and accumulated_content:
            # The output is already formatted, just return it
            return accumulated_content
        elif isinstance(accumulated_content, str):
            return [
                {
                    "role": "assistant",
                    "content": [{"type": "text", "text": accumulated_content}],
                }
            ]

    # Fallback for any other format
    return [
        {
            "role": "assistant",
            "content": [{"type": "text", "text": str(accumulated_content)}],
        }
    ]


def format_openai_streaming_input(
    kwargs: Dict[str, Any], api_type: str = "chat"
) -> Any:
    """
    Format OpenAI streaming input based on API type.

    Args:
        kwargs: Keyword arguments passed to OpenAI API
        api_type: Either "chat" or "responses"

    Returns:
        Formatted input ready for PostHog tracking
    """
    from posthog.ai.utils import merge_system_prompt

    return merge_system_prompt(kwargs, "openai")
