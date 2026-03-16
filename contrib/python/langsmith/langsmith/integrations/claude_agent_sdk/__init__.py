"""LangSmith integration for Claude Agent SDK.

This module provides automatic tracing for the Claude Agent SDK by instrumenting
ClaudeSDKClient and injecting hooks to trace all tool calls.
"""

import logging
import sys
from typing import Optional

from ._client import instrument_claude_client
from ._config import set_tracing_config

logger = logging.getLogger(__name__)

__all__ = ["configure_claude_agent_sdk"]


def configure_claude_agent_sdk(
    name: Optional[str] = None,
    project_name: Optional[str] = None,
    metadata: Optional[dict] = None,
    tags: Optional[list[str]] = None,
) -> bool:
    """Enable LangSmith tracing for the Claude Agent SDK by patching entry points.

    This function instruments the Claude Agent SDK to automatically trace:
    - Chain runs for each conversation stream (via ClaudeSDKClient)
    - Model runs for each assistant turn
    - All tool calls including built-in tools, external MCP tools, and SDK MCP tools

    Tool tracing is implemented via PreToolUse and PostToolUse hooks

    Args:
        name: Name of the root trace.
        project_name: LangSmith project to trace to.
        metadata: Metadata to associate with all traces.
        tags: Tags to associate with all traces.

    Returns:
        True if configuration was successful, False otherwise.

    Example:
        >>> from langsmith.integrations.claude_agents_sdk import (
        ...     configure_claude_agent_sdk,
        ... )
        >>> configure_claude_agent_sdk(project_name="my-project", tags=["production"])
        >>> # Now use claude_agent_sdk as normal - tracing is automatic
    """
    try:
        import claude_agent_sdk  # type: ignore[import-not-found]
    except ImportError:
        logger.warning("Claude Agent SDK not installed.")
        return False

    if not hasattr(claude_agent_sdk, "ClaudeSDKClient"):
        logger.warning("Claude Agent SDK missing ClaudeSDKClient.")
        return False

    set_tracing_config(
        name=name,
        project_name=project_name,
        metadata=metadata,
        tags=tags,
    )

    original = getattr(claude_agent_sdk, "ClaudeSDKClient", None)
    if not original:
        return False

    wrapped = instrument_claude_client(original)
    setattr(claude_agent_sdk, "ClaudeSDKClient", wrapped)

    for module in list(sys.modules.values()):
        try:
            if module and getattr(module, "ClaudeSDKClient", None) is original:
                setattr(module, "ClaudeSDKClient", wrapped)
        except Exception:
            continue

    return True
