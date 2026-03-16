"""
Anthropic-specific conversion utilities.

This module handles the conversion of Anthropic API responses and inputs
into standardized formats for PostHog tracking.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

from posthog.ai.types import (
    FormattedContentItem,
    FormattedFunctionCall,
    FormattedMessage,
    FormattedTextContent,
    StreamingContentBlock,
    TokenUsage,
    ToolInProgress,
)
from posthog.ai.utils import serialize_raw_usage


def format_anthropic_response(response: Any) -> List[FormattedMessage]:
    """
    Format an Anthropic response into standardized message format.

    Args:
        response: The response object from Anthropic API

    Returns:
        List of formatted messages with role and content
    """

    output: List[FormattedMessage] = []

    if response is None:
        return output

    content: List[FormattedContentItem] = []

    # Process content blocks from the response
    if hasattr(response, "content"):
        for choice in response.content:
            if (
                hasattr(choice, "type")
                and choice.type == "text"
                and hasattr(choice, "text")
                and choice.text
            ):
                text_content: FormattedTextContent = {
                    "type": "text",
                    "text": choice.text,
                }
                content.append(text_content)

            elif (
                hasattr(choice, "type")
                and choice.type == "tool_use"
                and hasattr(choice, "name")
                and hasattr(choice, "id")
            ):
                function_call: FormattedFunctionCall = {
                    "type": "function",
                    "id": choice.id,
                    "function": {
                        "name": choice.name,
                        "arguments": getattr(choice, "input", {}),
                    },
                }
                content.append(function_call)

    if content:
        message: FormattedMessage = {
            "role": "assistant",
            "content": content,
        }
        output.append(message)

    return output


def format_anthropic_input(
    messages: List[Dict[str, Any]], system: Optional[str] = None
) -> List[FormattedMessage]:
    """
    Format Anthropic input messages with optional system prompt.

    Args:
        messages: List of message dictionaries
        system: Optional system prompt to prepend

    Returns:
        List of formatted messages
    """

    formatted_messages: List[FormattedMessage] = []

    # Add system message if provided
    if system is not None:
        formatted_messages.append({"role": "system", "content": system})

    # Add user messages
    if messages:
        for msg in messages:
            # Messages are already in the correct format, just ensure type safety
            formatted_msg: FormattedMessage = {
                "role": msg.get("role", "user"),
                "content": msg.get("content", ""),
            }
            formatted_messages.append(formatted_msg)

    return formatted_messages


def extract_anthropic_tools(kwargs: Dict[str, Any]) -> Optional[Any]:
    """
    Extract tool definitions from Anthropic API kwargs.

    Args:
        kwargs: Keyword arguments passed to Anthropic API

    Returns:
        Tool definitions if present, None otherwise
    """

    return kwargs.get("tools", None)


def format_anthropic_streaming_content(
    content_blocks: List[StreamingContentBlock],
) -> List[FormattedContentItem]:
    """
    Format content blocks from Anthropic streaming response.

    Used by streaming handlers to format accumulated content blocks.

    Args:
        content_blocks: List of content block dictionaries from streaming

    Returns:
        List of formatted content items
    """

    formatted: List[FormattedContentItem] = []

    for block in content_blocks:
        if block.get("type") == "text":
            formatted.append(
                {
                    "type": "text",
                    "text": block.get("text") or "",
                }
            )

        elif block.get("type") == "function":
            formatted.append(
                {
                    "type": "function",
                    "id": block.get("id"),
                    "function": block.get("function") or {},
                }
            )

    return formatted


def extract_anthropic_web_search_count(response: Any) -> int:
    """
    Extract web search count from Anthropic response.

    Anthropic provides exact web search counts via usage.server_tool_use.web_search_requests.

    Args:
        response: The response from Anthropic API

    Returns:
        Number of web search requests (0 if none)
    """
    if not hasattr(response, "usage"):
        return 0

    if not hasattr(response.usage, "server_tool_use"):
        return 0

    server_tool_use = response.usage.server_tool_use

    if hasattr(server_tool_use, "web_search_requests"):
        return max(0, int(getattr(server_tool_use, "web_search_requests", 0)))

    return 0


def extract_anthropic_usage_from_response(response: Any) -> TokenUsage:
    """
    Extract usage from a full Anthropic response (non-streaming).

    Args:
        response: The complete response from Anthropic API

    Returns:
        TokenUsage with standardized usage
    """
    if not hasattr(response, "usage"):
        return TokenUsage(input_tokens=0, output_tokens=0)

    result = TokenUsage(
        input_tokens=getattr(response.usage, "input_tokens", 0),
        output_tokens=getattr(response.usage, "output_tokens", 0),
    )

    if hasattr(response.usage, "cache_read_input_tokens"):
        cache_read = response.usage.cache_read_input_tokens
        if cache_read and cache_read > 0:
            result["cache_read_input_tokens"] = cache_read

    if hasattr(response.usage, "cache_creation_input_tokens"):
        cache_creation = response.usage.cache_creation_input_tokens
        if cache_creation and cache_creation > 0:
            result["cache_creation_input_tokens"] = cache_creation

    web_search_count = extract_anthropic_web_search_count(response)
    if web_search_count > 0:
        result["web_search_count"] = web_search_count

    # Capture raw usage metadata for backend processing
    # Serialize to dict here in the converter (not in utils)
    serialized = serialize_raw_usage(response.usage)
    if serialized:
        result["raw_usage"] = serialized

    return result


def extract_anthropic_usage_from_event(event: Any) -> TokenUsage:
    """
    Extract usage statistics from an Anthropic streaming event.

    Args:
        event: Streaming event from Anthropic API

    Returns:
        Dictionary of usage statistics
    """

    usage: TokenUsage = TokenUsage()

    # Handle usage stats from message_start event
    if hasattr(event, "type") and event.type == "message_start":
        if hasattr(event, "message") and hasattr(event.message, "usage"):
            usage["input_tokens"] = getattr(event.message.usage, "input_tokens", 0)
            usage["cache_creation_input_tokens"] = getattr(
                event.message.usage, "cache_creation_input_tokens", 0
            )
            usage["cache_read_input_tokens"] = getattr(
                event.message.usage, "cache_read_input_tokens", 0
            )
            # Capture raw usage metadata for backend processing
            # Serialize to dict here in the converter (not in utils)
            serialized = serialize_raw_usage(event.message.usage)
            if serialized:
                usage["raw_usage"] = serialized

    # Handle usage stats from message_delta event
    if hasattr(event, "usage") and event.usage:
        usage["output_tokens"] = getattr(event.usage, "output_tokens", 0)

        # Extract web search count from usage
        if hasattr(event.usage, "server_tool_use"):
            server_tool_use = event.usage.server_tool_use
            if hasattr(server_tool_use, "web_search_requests"):
                web_search_count = int(
                    getattr(server_tool_use, "web_search_requests", 0)
                )
                if web_search_count > 0:
                    usage["web_search_count"] = web_search_count

        # Capture raw usage metadata for backend processing
        # Serialize to dict here in the converter (not in utils)
        serialized = serialize_raw_usage(event.usage)
        if serialized:
            usage["raw_usage"] = serialized

    return usage


def handle_anthropic_content_block_start(
    event: Any,
) -> Tuple[Optional[StreamingContentBlock], Optional[ToolInProgress]]:
    """
    Handle content block start event from Anthropic streaming.

    Args:
        event: Content block start event

    Returns:
        Tuple of (content_block, tool_in_progress)
    """

    if not (hasattr(event, "type") and event.type == "content_block_start"):
        return None, None

    if not hasattr(event, "content_block"):
        return None, None

    block = event.content_block

    if not hasattr(block, "type"):
        return None, None

    if block.type == "text":
        content_block: StreamingContentBlock = {"type": "text", "text": ""}
        return content_block, None

    elif block.type == "tool_use":
        tool_block: StreamingContentBlock = {
            "type": "function",
            "id": getattr(block, "id", ""),
            "function": {"name": getattr(block, "name", ""), "arguments": {}},
        }
        tool_in_progress: ToolInProgress = {"block": tool_block, "input_string": ""}
        return tool_block, tool_in_progress

    return None, None


def handle_anthropic_text_delta(
    event: Any, current_block: Optional[StreamingContentBlock]
) -> Optional[str]:
    """
    Handle text delta event from Anthropic streaming.

    Args:
        event: Delta event
        current_block: Current text block being accumulated

    Returns:
        Text delta if present
    """

    if hasattr(event, "delta") and hasattr(event.delta, "text"):
        delta_text = event.delta.text or ""

        if current_block is not None and current_block.get("type") == "text":
            text_val = current_block.get("text")
            if text_val is not None:
                current_block["text"] = text_val + delta_text
            else:
                current_block["text"] = delta_text

        return delta_text

    return None


def handle_anthropic_tool_delta(
    event: Any,
    content_blocks: List[StreamingContentBlock],
    tools_in_progress: Dict[str, ToolInProgress],
) -> None:
    """
    Handle tool input delta event from Anthropic streaming.

    Args:
        event: Tool delta event
        content_blocks: List of content blocks
        tools_in_progress: Dictionary tracking tools being accumulated
    """

    if not (hasattr(event, "type") and event.type == "content_block_delta"):
        return

    if not (
        hasattr(event, "delta")
        and hasattr(event.delta, "type")
        and event.delta.type == "input_json_delta"
    ):
        return

    if hasattr(event, "index") and event.index < len(content_blocks):
        block = content_blocks[event.index]

        if block.get("type") == "function" and block.get("id") in tools_in_progress:
            tool = tools_in_progress[block["id"]]
            partial_json = getattr(event.delta, "partial_json", "")
            tool["input_string"] += partial_json


def finalize_anthropic_tool_input(
    event: Any,
    content_blocks: List[StreamingContentBlock],
    tools_in_progress: Dict[str, ToolInProgress],
) -> None:
    """
    Finalize tool input when content block stops.

    Args:
        event: Content block stop event
        content_blocks: List of content blocks
        tools_in_progress: Dictionary tracking tools being accumulated
    """

    if not (hasattr(event, "type") and event.type == "content_block_stop"):
        return

    if hasattr(event, "index") and event.index < len(content_blocks):
        block = content_blocks[event.index]

        if block.get("type") == "function" and block.get("id") in tools_in_progress:
            tool = tools_in_progress[block["id"]]

            try:
                block["function"]["arguments"] = json.loads(tool["input_string"])
            except (json.JSONDecodeError, Exception):
                # Keep empty dict if parsing fails
                pass

            del tools_in_progress[block["id"]]


def format_anthropic_streaming_input(kwargs: Dict[str, Any]) -> Any:
    """
    Format Anthropic streaming input using system prompt merging.

    Args:
        kwargs: Keyword arguments passed to Anthropic API

    Returns:
        Formatted input ready for PostHog tracking
    """
    from posthog.ai.utils import merge_system_prompt

    return merge_system_prompt(kwargs, "anthropic")


def format_anthropic_streaming_output_complete(
    content_blocks: List[StreamingContentBlock], accumulated_content: str
) -> List[FormattedMessage]:
    """
    Format complete Anthropic streaming output.

    Combines existing logic for formatting content blocks with fallback to accumulated content.

    Args:
        content_blocks: List of content blocks accumulated during streaming
        accumulated_content: Raw accumulated text content as fallback

    Returns:
        Formatted messages ready for PostHog tracking
    """
    formatted_content = format_anthropic_streaming_content(content_blocks)

    if formatted_content:
        return [{"role": "assistant", "content": formatted_content}]
    else:
        # Fallback to accumulated content if no blocks
        return [
            {
                "role": "assistant",
                "content": [{"type": "text", "text": accumulated_content}],
            }
        ]
