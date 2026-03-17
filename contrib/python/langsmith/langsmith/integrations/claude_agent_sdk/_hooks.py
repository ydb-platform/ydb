"""Hook-based tool tracing for Claude Agent SDK.

This module provides hook handlers that traces tool calls by intercepting
PreToolUse and PostToolUse events.
"""

import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

from langsmith.run_helpers import get_current_run_tree
from langsmith.run_trees import RunTree

from ._tools import get_parent_run_tree

if TYPE_CHECKING:
    from claude_agent_sdk import (
        HookContext,
        HookInput,
        HookJSONOutput,
    )

logger = logging.getLogger(__name__)

# Storage for correlating PreToolUse and PostToolUse events
# Key: tool_use_id, Value: (run_tree, start_time)
_active_tool_runs: dict[str, tuple[Any, float]] = {}

# Storage for tool or subagent runs managed by client
# Key: tool_use_id, Value: run_tree
_client_managed_runs: dict[str, RunTree] = {}


async def pre_tool_use_hook(
    input_data: "HookInput",
    tool_use_id: Optional[str],
    context: "HookContext",
) -> "HookJSONOutput":
    """Trace tool execution before it starts.

    Args:
        input_data: Contains tool_name, tool_input, session_id
        tool_use_id: Unique identifier for this tool invocation
        context: Hook context (currently contains only signal)

    Returns:
        Hook output (empty dict allows execution to proceed)
    """
    if not tool_use_id:
        logger.debug("PreToolUse hook called without tool_use_id, skipping trace")
        return {}

    # Skip if this tool run is already managed by the client
    if tool_use_id in _client_managed_runs:
        return {}

    tool_name: str = str(input_data.get("tool_name", "unknown_tool"))
    tool_input = input_data.get("tool_input", {})

    try:
        parent = get_parent_run_tree() or get_current_run_tree()
        if not parent:
            logger.debug(f"No parent run tree found for tool {tool_name}")
            return {}

        start_time = time.time()
        tool_run = parent.create_child(
            name=tool_name,
            run_type="tool",
            inputs={"input": tool_input} if tool_input else {},
            start_time=datetime.fromtimestamp(start_time, tz=timezone.utc),
        )

        try:
            tool_run.post()
        except Exception as e:
            logger.warning(f"Failed to post tool run for {tool_name}: {e}")

        _active_tool_runs[tool_use_id] = (tool_run, start_time)
        logger.debug(f"Started tool trace for {tool_name} (id={tool_use_id})")

    except Exception as e:
        logger.warning(f"Error in PreToolUse hook for {tool_name}: {e}", exc_info=True)

    return {}


async def post_tool_use_hook(
    input_data: "HookInput",
    tool_use_id: Optional[str],
    context: "HookContext",
) -> "HookJSONOutput":
    """Trace tool execution after it completes.

    Args:
        input_data: Contains tool_name, tool_input, tool_response, session_id, etc.
        tool_use_id: Unique identifier for this tool invocation
        context: Hook context (currently contains only signal)

    Returns:
        Hook output (empty dict by default)
    """
    if not tool_use_id:
        logger.debug("PostToolUse hook called without tool_use_id, skipping trace")
        return {}

    tool_name: str = str(input_data.get("tool_name", "unknown_tool"))
    tool_response = input_data.get("tool_response")

    # Check if this is a client-managed run
    run_tree = _client_managed_runs.pop(tool_use_id, None)
    if run_tree:
        # This run is managed by the client (subagent session or its tools)
        try:
            if isinstance(tool_response, dict):
                outputs = tool_response
            elif isinstance(tool_response, list):
                outputs = {"content": tool_response}
            else:
                outputs = {"output": str(tool_response)} if tool_response else {}

            is_error = False
            if isinstance(tool_response, dict):
                is_error = tool_response.get("is_error", False)

            run_tree.end(
                outputs=outputs,
                error=outputs.get("output") if is_error else None,
            )
            run_tree.patch()
        except Exception as e:
            logger.warning(f"Failed to update client-managed run: {e}")
        return {}

    try:
        run_info = _active_tool_runs.pop(tool_use_id, None)
        if not run_info:
            logger.debug(
                f"No matching PreToolUse found for {tool_name} (id={tool_use_id})"
            )
            return {}

        tool_run, start_time = run_info

        if isinstance(tool_response, dict):
            outputs = tool_response
        elif isinstance(tool_response, list):
            outputs = {"content": tool_response}
        else:
            outputs = {"output": str(tool_response)} if tool_response else {}

        # Check if the tool execution was an error
        is_error = False
        if isinstance(tool_response, dict):
            is_error = tool_response.get("is_error", False)

        tool_run.end(
            outputs=outputs,
            error=outputs.get("output") if is_error else None,
        )

        try:
            tool_run.patch()
        except Exception as e:
            logger.warning(f"Failed to patch tool run for {tool_name}: {e}")

        duration_ms = (time.time() - start_time) * 1000
        logger.debug(
            f"Completed tool trace for {tool_name} "
            f"(id={tool_use_id}, duration={duration_ms:.2f}ms)"
        )

    except Exception as e:
        logger.warning(f"Error in PostToolUse hook for {tool_name}: {e}", exc_info=True)

    return {}


def clear_active_tool_runs() -> None:
    """Clear all active tool runs.

    This should be called when a conversation ends to avoid memory leaks
    and to clean up any orphaned tool runs.
    """
    global _active_tool_runs, _client_managed_runs

    # End any orphaned client-managed runs
    for tool_use_id, run_tree in _client_managed_runs.items():
        try:
            run_tree.end(error="Client-managed run not completed (conversation ended)")
            run_tree.patch()
        except Exception as e:
            logger.debug(
                f"Failed to clean up orphaned client-managed run {tool_use_id}: {e}"
            )

    # End any orphaned tool runs
    for tool_use_id, (tool_run, _) in _active_tool_runs.items():
        try:
            tool_run.end(error="Tool run not completed (conversation ended)")
            tool_run.patch()
        except Exception as e:
            logger.debug(f"Failed to clean up orphaned tool run {tool_use_id}: {e}")

    _active_tool_runs.clear()
    _client_managed_runs.clear()
