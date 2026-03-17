"""Message processing and content serialization for Claude Agent SDK."""

from typing import Any


def flatten_content_blocks(content: Any) -> Any:
    """Convert SDK content blocks into serializable dicts using explicit type checks."""
    if not isinstance(content, list):
        return content

    result = []
    for block in content:
        block_type = type(block).__name__

        # Handle known Claude SDK block types
        if block_type == "TextBlock":
            result.append(
                {
                    "type": "text",
                    "text": getattr(block, "text", ""),
                }
            )
        elif block_type == "ThinkingBlock":
            result.append(
                {
                    "type": "thinking",
                    "thinking": getattr(block, "thinking", ""),
                    "signature": getattr(block, "signature", ""),
                }
            )
        elif block_type == "ToolUseBlock":
            result.append(
                {
                    "type": "tool_use",
                    "id": getattr(block, "id", None),
                    "name": getattr(block, "name", None),
                    "input": getattr(block, "input", None),
                }
            )
        elif block_type == "ToolResultBlock":
            result.append(
                {
                    "type": "tool_result",
                    "tool_use_id": getattr(block, "tool_use_id", None),
                    "content": getattr(block, "content", None),
                    "is_error": getattr(block, "is_error", False),
                }
            )
        else:
            result.append(block)
    return result


def build_llm_input(prompt: Any, history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Construct a combined prompt + history message list."""
    if isinstance(prompt, str):
        entry = {"content": prompt, "role": "user"}
        return [entry, *history] if history else [entry]
    return history or []


def extract_usage_from_result_message(msg: Any) -> dict[str, Any]:
    """Normalize and merge token usage metrics from a ResultMessage."""
    from ._usage import extract_usage_metadata, sum_anthropic_tokens

    if not getattr(msg, "usage", None):
        return {}
    metrics = extract_usage_metadata(msg.usage)
    return sum_anthropic_tokens(metrics) if metrics else {}
