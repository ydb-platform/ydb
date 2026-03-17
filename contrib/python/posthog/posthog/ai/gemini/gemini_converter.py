"""
Gemini-specific conversion utilities.

This module handles the conversion of Gemini API responses and inputs
into standardized formats for PostHog tracking.
"""

from typing import Any, Dict, List, Optional, TypedDict, Union

from posthog.ai.types import (
    FormattedContentItem,
    FormattedMessage,
    TokenUsage,
)
from posthog.ai.utils import serialize_raw_usage


class GeminiPart(TypedDict, total=False):
    """Represents a part in a Gemini message."""

    text: str


class GeminiMessage(TypedDict, total=False):
    """Represents a Gemini message with various possible fields."""

    role: str
    parts: List[Union[GeminiPart, Dict[str, Any]]]
    content: Union[str, List[Any]]
    text: str


def _format_parts_as_content_blocks(parts: List[Any]) -> List[FormattedContentItem]:
    """
    Format Gemini parts array into structured content blocks.

    Preserves structure for multimodal content (text + images) instead of
    concatenating everything into a string.

    Args:
        parts: List of parts that may contain text, inline_data, etc.

    Returns:
        List of formatted content blocks
    """
    content_blocks: List[FormattedContentItem] = []

    for part in parts:
        # Handle dict with text field
        if isinstance(part, dict) and "text" in part:
            content_blocks.append({"type": "text", "text": part["text"]})

        # Handle string parts
        elif isinstance(part, str):
            content_blocks.append({"type": "text", "text": part})

        # Handle dict with inline_data (images, documents, etc.)
        elif isinstance(part, dict) and "inline_data" in part:
            inline_data = part["inline_data"]
            mime_type = inline_data.get("mime_type", "")
            content_type = "image" if mime_type.startswith("image/") else "document"

            content_blocks.append(
                {
                    "type": content_type,
                    "inline_data": inline_data,
                }
            )

        # Handle object with text attribute
        elif hasattr(part, "text"):
            text_value = getattr(part, "text", "")
            if text_value:
                content_blocks.append({"type": "text", "text": text_value})

        # Handle object with inline_data attribute
        elif hasattr(part, "inline_data"):
            inline_data = part.inline_data
            # Convert to dict if needed
            if hasattr(inline_data, "mime_type") and hasattr(inline_data, "data"):
                # Determine type based on mime_type
                mime_type = inline_data.mime_type
                content_type = "image" if mime_type.startswith("image/") else "document"

                content_blocks.append(
                    {
                        "type": content_type,
                        "inline_data": {
                            "mime_type": mime_type,
                            "data": inline_data.data,
                        },
                    }
                )
            else:
                content_blocks.append(
                    {
                        "type": "image",
                        "inline_data": inline_data,
                    }
                )

    return content_blocks


def _format_dict_message(item: Dict[str, Any]) -> FormattedMessage:
    """
    Format a dictionary message into standardized format.

    Args:
        item: Dictionary containing message data

    Returns:
        Formatted message with role and content
    """

    # Handle dict format with parts array (Gemini-specific format)
    if "parts" in item and isinstance(item["parts"], list):
        content_blocks = _format_parts_as_content_blocks(item["parts"])
        return {"role": item.get("role", "user"), "content": content_blocks}

    # Handle dict with content field
    if "content" in item:
        content = item["content"]

        if isinstance(content, list):
            # If content is a list, format it as content blocks
            content_blocks = _format_parts_as_content_blocks(content)
            return {"role": item.get("role", "user"), "content": content_blocks}

        elif not isinstance(content, str):
            content = str(content)

        return {"role": item.get("role", "user"), "content": content}

    # Handle dict with text field
    if "text" in item:
        return {"role": item.get("role", "user"), "content": item["text"]}

    # Fallback to string representation
    return {"role": "user", "content": str(item)}


def _format_object_message(item: Any) -> FormattedMessage:
    """
    Format an object (with attributes) into standardized format.

    Args:
        item: Object that may have text or parts attributes

    Returns:
        Formatted message with role and content
    """

    # Handle object with parts attribute
    if hasattr(item, "parts") and hasattr(item.parts, "__iter__"):
        content_blocks = _format_parts_as_content_blocks(list(item.parts))
        role = getattr(item, "role", "user") if hasattr(item, "role") else "user"

        # Ensure role is a string
        if not isinstance(role, str):
            role = "user"

        return {"role": role, "content": content_blocks}

    # Handle object with text attribute
    if hasattr(item, "text"):
        role = getattr(item, "role", "user") if hasattr(item, "role") else "user"

        # Ensure role is a string
        if not isinstance(role, str):
            role = "user"

        return {"role": role, "content": item.text}

    # Handle object with content attribute
    if hasattr(item, "content"):
        role = getattr(item, "role", "user") if hasattr(item, "role") else "user"

        # Ensure role is a string
        if not isinstance(role, str):
            role = "user"

        content = item.content

        if isinstance(content, list):
            content_blocks = _format_parts_as_content_blocks(content)
            return {"role": role, "content": content_blocks}

        elif not isinstance(content, str):
            content = str(content)
        return {"role": role, "content": content}

    # Fallback to string representation
    return {"role": "user", "content": str(item)}


def format_gemini_response(response: Any) -> List[FormattedMessage]:
    """
    Format a Gemini response into standardized message format.

    Args:
        response: The response object from Gemini API

    Returns:
        List of formatted messages with role and content
    """

    output: List[FormattedMessage] = []

    if response is None:
        return output

    if hasattr(response, "candidates") and response.candidates:
        for candidate in response.candidates:
            if hasattr(candidate, "content") and candidate.content:
                content: List[FormattedContentItem] = []

                if hasattr(candidate.content, "parts") and candidate.content.parts:
                    for part in candidate.content.parts:
                        if hasattr(part, "text") and part.text:
                            content.append(
                                {
                                    "type": "text",
                                    "text": part.text,
                                }
                            )

                        elif hasattr(part, "function_call") and part.function_call:
                            function_call = part.function_call
                            content.append(
                                {
                                    "type": "function",
                                    "function": {
                                        "name": function_call.name,
                                        "arguments": function_call.args,
                                    },
                                }
                            )

                        elif hasattr(part, "inline_data") and part.inline_data:
                            # Handle audio/media inline data
                            import base64

                            inline_data = part.inline_data
                            mime_type = getattr(inline_data, "mime_type", "audio/pcm")
                            raw_data = getattr(inline_data, "data", b"")

                            # Encode binary data as base64 string for JSON serialization
                            if isinstance(raw_data, bytes):
                                data = base64.b64encode(raw_data).decode("utf-8")
                            else:
                                # Already a string (base64)
                                data = raw_data

                            content.append(
                                {
                                    "type": "audio",
                                    "mime_type": mime_type,
                                    "data": data,
                                }
                            )

                if content:
                    output.append(
                        {
                            "role": "assistant",
                            "content": content,
                        }
                    )

            elif hasattr(candidate, "text") and candidate.text:
                output.append(
                    {
                        "role": "assistant",
                        "content": [{"type": "text", "text": candidate.text}],
                    }
                )

    elif hasattr(response, "text") and response.text:
        output.append(
            {
                "role": "assistant",
                "content": [{"type": "text", "text": response.text}],
            }
        )

    return output


def extract_gemini_system_instruction(config: Any) -> Optional[str]:
    """
    Extract system instruction from Gemini config parameter.

    Args:
        config: Config object or dict that may contain system instruction

    Returns:
        System instruction string if present, None otherwise
    """
    if config is None:
        return None

    # Handle different config formats
    if hasattr(config, "system_instruction"):
        return config.system_instruction
    elif isinstance(config, dict) and "system_instruction" in config:
        return config["system_instruction"]
    elif isinstance(config, dict) and "systemInstruction" in config:
        return config["systemInstruction"]

    return None


def extract_gemini_tools(kwargs: Dict[str, Any]) -> Optional[Any]:
    """
    Extract tool definitions from Gemini API kwargs.

    Args:
        kwargs: Keyword arguments passed to Gemini API

    Returns:
        Tool definitions if present, None otherwise
    """

    if "config" in kwargs and hasattr(kwargs["config"], "tools"):
        return kwargs["config"].tools

    return None


def format_gemini_input_with_system(
    contents: Any, config: Any = None
) -> List[FormattedMessage]:
    """
    Format Gemini input contents into standardized message format, including system instruction handling.

    Args:
        contents: Input contents in various possible formats
        config: Config object or dict that may contain system instruction

    Returns:
        List of formatted messages with role and content fields, with system message prepended if needed
    """
    formatted_messages = format_gemini_input(contents)

    # Check if system instruction is provided in config parameter
    system_instruction = extract_gemini_system_instruction(config)

    if system_instruction is not None:
        has_system = any(msg.get("role") == "system" for msg in formatted_messages)
        if not has_system:
            from posthog.ai.types import FormattedMessage

            system_message: FormattedMessage = {
                "role": "system",
                "content": system_instruction,
            }
            formatted_messages = [system_message] + list(formatted_messages)

    return formatted_messages


def format_gemini_input(contents: Any) -> List[FormattedMessage]:
    """
    Format Gemini input contents into standardized message format for PostHog tracking.

    This function handles various input formats:
    - String inputs
    - List of strings, dicts, or objects
    - Single dict or object
    - Gemini-specific format with parts array

    Args:
        contents: Input contents in various possible formats

    Returns:
        List of formatted messages with role and content fields
    """

    # Handle string input
    if isinstance(contents, str):
        return [{"role": "user", "content": contents}]

    # Handle list input
    if isinstance(contents, list):
        formatted: List[FormattedMessage] = []

        for item in contents:
            if isinstance(item, str):
                formatted.append({"role": "user", "content": item})

            elif isinstance(item, dict):
                formatted.append(_format_dict_message(item))

            else:
                formatted.append(_format_object_message(item))

        return formatted

    # Handle single dict input
    if isinstance(contents, dict):
        return [_format_dict_message(contents)]

    # Handle single object input
    return [_format_object_message(contents)]


def extract_gemini_web_search_count(response: Any) -> int:
    """
    Extract web search count from Gemini response.

    Gemini bills per request that uses grounding, not per query.
    Returns 1 if grounding_metadata is present with actual search data, 0 otherwise.

    Args:
        response: The response from Gemini API

    Returns:
        1 if web search/grounding was used, 0 otherwise
    """

    # Check for grounding_metadata in candidates
    if hasattr(response, "candidates"):
        for candidate in response.candidates:
            if (
                hasattr(candidate, "grounding_metadata")
                and candidate.grounding_metadata
            ):
                grounding_metadata = candidate.grounding_metadata

                # Check if web_search_queries exists and is non-empty
                if hasattr(grounding_metadata, "web_search_queries"):
                    queries = grounding_metadata.web_search_queries

                    if queries is not None and len(queries) > 0:
                        return 1

                # Check if grounding_chunks exists and is non-empty
                if hasattr(grounding_metadata, "grounding_chunks"):
                    chunks = grounding_metadata.grounding_chunks

                    if chunks is not None and len(chunks) > 0:
                        return 1

            # Also check for google_search or grounding in function call names
            if hasattr(candidate, "content") and candidate.content:
                if hasattr(candidate.content, "parts") and candidate.content.parts:
                    for part in candidate.content.parts:
                        if hasattr(part, "function_call") and part.function_call:
                            function_name = getattr(
                                part.function_call, "name", ""
                            ).lower()

                            if (
                                "google_search" in function_name
                                or "grounding" in function_name
                            ):
                                return 1

    return 0


def _extract_usage_from_metadata(metadata: Any) -> TokenUsage:
    """
    Common logic to extract usage from Gemini metadata.
    Used by both streaming and non-streaming paths.

    Args:
        metadata: usage_metadata from Gemini response or chunk

    Returns:
        TokenUsage with standardized usage
    """
    usage = TokenUsage(
        input_tokens=getattr(metadata, "prompt_token_count", 0),
        output_tokens=getattr(metadata, "candidates_token_count", 0),
    )

    # Add cache tokens if present (don't add if 0)
    if hasattr(metadata, "cached_content_token_count"):
        cache_tokens = metadata.cached_content_token_count
        if cache_tokens and cache_tokens > 0:
            usage["cache_read_input_tokens"] = cache_tokens

    # Add reasoning tokens if present (don't add if 0)
    if hasattr(metadata, "thoughts_token_count"):
        reasoning_tokens = metadata.thoughts_token_count
        if reasoning_tokens and reasoning_tokens > 0:
            usage["reasoning_tokens"] = reasoning_tokens

    # Capture raw usage metadata for backend processing
    # Serialize to dict here in the converter (not in utils)
    serialized = serialize_raw_usage(metadata)
    if serialized:
        usage["raw_usage"] = serialized

    return usage


def extract_gemini_usage_from_response(response: Any) -> TokenUsage:
    """
    Extract usage statistics from a full Gemini response (non-streaming).

    Args:
        response: The complete response from Gemini API

    Returns:
        TokenUsage with standardized usage statistics
    """
    if not hasattr(response, "usage_metadata") or not response.usage_metadata:
        return TokenUsage(input_tokens=0, output_tokens=0)

    usage = _extract_usage_from_metadata(response.usage_metadata)

    # Add web search count if present
    web_search_count = extract_gemini_web_search_count(response)
    if web_search_count > 0:
        usage["web_search_count"] = web_search_count

    return usage


def extract_gemini_usage_from_chunk(chunk: Any) -> TokenUsage:
    """
    Extract usage statistics from a Gemini streaming chunk.

    Args:
        chunk: Streaming chunk from Gemini API

    Returns:
        TokenUsage with standardized usage statistics
    """

    usage: TokenUsage = TokenUsage()

    # Extract web search count from the chunk before checking for usage_metadata
    # Web search indicators can appear on any chunk, not just those with usage data
    web_search_count = extract_gemini_web_search_count(chunk)
    if web_search_count > 0:
        usage["web_search_count"] = web_search_count

    if not hasattr(chunk, "usage_metadata") or not chunk.usage_metadata:
        return usage

    usage_from_metadata = _extract_usage_from_metadata(chunk.usage_metadata)

    # Merge the usage from metadata with any web search count we found
    usage.update(usage_from_metadata)

    return usage


def extract_gemini_content_from_chunk(chunk: Any) -> Optional[Dict[str, Any]]:
    """
    Extract content (text or function call) from a Gemini streaming chunk.

    Args:
        chunk: Streaming chunk from Gemini API

    Returns:
        Content block dictionary if present, None otherwise
    """

    # Check for text content
    if hasattr(chunk, "text") and chunk.text:
        return {"type": "text", "text": chunk.text}

    # Check for function calls in candidates
    if hasattr(chunk, "candidates") and chunk.candidates:
        for candidate in chunk.candidates:
            if hasattr(candidate, "content") and candidate.content:
                if hasattr(candidate.content, "parts") and candidate.content.parts:
                    for part in candidate.content.parts:
                        # Check for function_call part
                        if hasattr(part, "function_call") and part.function_call:
                            function_call = part.function_call
                            return {
                                "type": "function",
                                "function": {
                                    "name": function_call.name,
                                    "arguments": function_call.args,
                                },
                            }
                        # Also check for text in parts
                        elif hasattr(part, "text") and part.text:
                            return {"type": "text", "text": part.text}

    return None


def format_gemini_streaming_output(
    accumulated_content: Union[str, List[Any]],
) -> List[FormattedMessage]:
    """
    Format the final output from Gemini streaming.

    Args:
        accumulated_content: Accumulated content from streaming (string, list of strings, or list of content blocks)

    Returns:
        List of formatted messages
    """

    # Handle legacy string input (backward compatibility)
    if isinstance(accumulated_content, str):
        return [
            {
                "role": "assistant",
                "content": [{"type": "text", "text": accumulated_content}],
            }
        ]

    # Handle list input
    if isinstance(accumulated_content, list):
        content: List[FormattedContentItem] = []
        text_parts = []

        for item in accumulated_content:
            if isinstance(item, str):
                # Legacy support: accumulate strings
                text_parts.append(item)
            elif isinstance(item, dict):
                # New format: content blocks
                if item.get("type") == "text":
                    text_parts.append(item.get("text", ""))
                elif item.get("type") == "function":
                    # If we have accumulated text, add it first
                    if text_parts:
                        content.append(
                            {
                                "type": "text",
                                "text": "".join(text_parts),
                            }
                        )
                        text_parts = []

                    # Add the function call
                    content.append(
                        {
                            "type": "function",
                            "function": item.get("function", {}),
                        }
                    )

        # Add any remaining text
        if text_parts:
            content.append(
                {
                    "type": "text",
                    "text": "".join(text_parts),
                }
            )

        # If we have content, return it
        if content:
            return [{"role": "assistant", "content": content}]

    # Fallback for empty or unexpected input
    return [{"role": "assistant", "content": [{"type": "text", "text": ""}]}]
