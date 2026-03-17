"""Client instrumentation for Claude Agent SDK."""

import logging
import time
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, Optional

from langsmith.run_helpers import get_current_run_tree, trace

from ._hooks import clear_active_tool_runs, post_tool_use_hook, pre_tool_use_hook
from ._messages import (
    build_llm_input,
    extract_usage_from_result_message,
    flatten_content_blocks,
)
from ._tools import clear_parent_run_tree, set_parent_run_tree

logger = logging.getLogger(__name__)

TRACE_CHAIN_NAME = "claude.conversation"
LLM_RUN_NAME = "claude.assistant.turn"


class TurnLifecycle:
    """Track ongoing model runs so consecutive messages are recorded correctly."""

    def __init__(self, query_start_time: Optional[float] = None):
        self.current_run: Optional[Any] = None
        self.next_start_time: Optional[float] = query_start_time

    def start_llm_run(
        self, message: Any, prompt: Any, history: list[dict[str, Any]]
    ) -> Optional[dict[str, Any]]:
        """Begin a new model run, ending any existing one."""
        start = self.next_start_time or time.time()

        if self.current_run:
            self.current_run.end()
            self.current_run.patch()

        final_output, run = begin_llm_run_from_assistant_messages(
            [message], prompt, history, start_time=start
        )
        self.current_run = run
        self.next_start_time = None
        return final_output

    def mark_next_start(self) -> None:
        """Mark when the next assistant message will start."""
        self.next_start_time = time.time()

    def add_usage(self, metrics: dict[str, Any]) -> None:
        """Attach token usage details to the current run."""
        if not (self.current_run and metrics):
            return
        meta = self.current_run.extra.setdefault("metadata", {}).setdefault(
            "usage_metadata", {}
        )
        meta.update(metrics)
        try:
            self.current_run.patch()
        except Exception as e:
            logger.warning(f"Failed to update usage metrics: {e}")

    def close(self) -> None:
        """End any open run gracefully."""
        if self.current_run:
            self.current_run.end()
            self.current_run.patch()
            self.current_run = None


def begin_llm_run_from_assistant_messages(
    messages: list[Any],
    prompt: Any,
    history: list[dict[str, Any]],
    start_time: Optional[float] = None,
) -> tuple[Optional[dict[str, Any]], Optional[Any]]:
    """Create a traced model run from assistant messages."""
    if not messages or type(messages[-1]).__name__ != "AssistantMessage":
        return None, None

    last_msg = messages[-1]
    model = getattr(last_msg, "model", None)
    parent = get_current_run_tree()
    if not parent:
        return None, None

    inputs = build_llm_input(prompt, history)
    outputs = [
        {"content": flatten_content_blocks(m.content), "role": "assistant"}
        for m in messages
        if hasattr(m, "content")
    ]

    llm_run = parent.create_child(
        name=LLM_RUN_NAME,
        run_type="llm",
        inputs=inputs if len(inputs) > 1 else inputs[0] if inputs else {},  # type: ignore[arg-type]
        outputs=outputs[-1] if len(outputs) == 1 else {"content": outputs},
        extra={"metadata": {"ls_model_name": model}} if model else {},
        start_time=datetime.fromtimestamp(start_time, tz=timezone.utc)
        if start_time
        else None,
    )

    try:
        llm_run.post()
    except Exception as e:
        logger.warning(f"Failed to post LLM run: {e}")

    final_content = (
        {"content": flatten_content_blocks(last_msg.content), "role": "assistant"}
        if hasattr(last_msg, "content")
        else None
    )
    return final_content, llm_run


def _inject_tracing_hooks(options: Any) -> None:
    """Inject LangSmith tracing hooks into ClaudeAgentOptions.

    This adds PreToolUse and PostToolUse hooks to capture ALL tool calls
    (built-in, external MCP, and SDK MCP). The hooks work across all LLM
    providers (Anthropic, Vertex AI, Kimi, etc.) because they use explicit
    tool_use_id correlation instead of relying on async context propagation.

    Args:
        options: ClaudeAgentOptions instance to modify
    """
    if not hasattr(options, "hooks"):
        return

    # Initialize hooks dict if not present
    if options.hooks is None:
        options.hooks = {}

    # Add PreToolUse hook if not already set
    if "PreToolUse" not in options.hooks:
        options.hooks["PreToolUse"] = []

    # Add PostToolUse hook if not already set
    if "PostToolUse" not in options.hooks:
        options.hooks["PostToolUse"] = []

    try:
        from claude_agent_sdk import HookMatcher  # type: ignore[import-not-found]

        langsmith_pre_matcher = HookMatcher(matcher=None, hooks=[pre_tool_use_hook])
        langsmith_post_matcher = HookMatcher(matcher=None, hooks=[post_tool_use_hook])

        options.hooks["PreToolUse"].insert(0, langsmith_pre_matcher)
        options.hooks["PostToolUse"].insert(0, langsmith_post_matcher)

        logger.debug("Injected LangSmith tracing hooks into ClaudeAgentOptions")
    except ImportError:
        logger.warning("Failed to import HookMatcher from claude_agent_sdk")
    except Exception as e:
        logger.warning(f"Failed to inject tracing hooks: {e}")


def instrument_claude_client(original_class: Any) -> Any:
    """Wrap ClaudeSDKClient to trace both query() and receive_response()."""

    class TracedClaudeSDKClient(original_class):
        def __init__(self, *args: Any, **kwargs: Any):
            # Inject LangSmith tracing hooks into options before initialization
            options = kwargs.get("options") or (args[0] if args else None)
            if options:
                _inject_tracing_hooks(options)

            super().__init__(*args, **kwargs)
            self._prompt: Optional[str] = None
            self._start_time: Optional[float] = None

        async def query(self, *args: Any, **kwargs: Any) -> Any:
            """Capture prompt and timestamp when query starts."""
            self._start_time = time.time()
            self._prompt = str(kwargs.get("prompt") or (args[0] if args else ""))
            return await super().query(*args, **kwargs)

        def _handle_assistant_tool_uses(
            self,
            msg: Any,
            run: Any,
            subagent_sessions: dict[str, Any],
        ) -> None:
            """Process tool uses for an assistant message."""
            if not hasattr(msg, "content"):
                return

            from ._hooks import _client_managed_runs

            parent_tool_use_id = getattr(msg, "parent_tool_use_id", None)

            for block in msg.content:
                if type(block).__name__ != "ToolUseBlock":
                    continue

                try:
                    tool_use_id = getattr(block, "id", None)
                    tool_name = getattr(block, "name", "unknown_tool")
                    tool_input = getattr(block, "input", {})

                    if not tool_use_id:
                        continue

                    start_time = time.time()

                    # Check if this is a Task tool (subagent)
                    if tool_name == "Task" and not parent_tool_use_id:
                        # Extract subagent name
                        subagent_name = (
                            tool_input.get("subagent_type")
                            or (
                                tool_input.get("description", "").split()[0]
                                if tool_input.get("description")
                                else None
                            )
                            or "unknown-agent"
                        )

                        subagent_session = run.create_child(
                            name=subagent_name,
                            run_type="chain",
                            start_time=datetime.fromtimestamp(
                                start_time, tz=timezone.utc
                            ),
                        )
                        subagent_session.post()
                        subagent_sessions[tool_use_id] = subagent_session

                        _client_managed_runs[tool_use_id] = subagent_session

                    # Check if tool use is within a subagent
                    elif parent_tool_use_id and parent_tool_use_id in subagent_sessions:
                        subagent_session = subagent_sessions[parent_tool_use_id]
                        # Create tool run as child of subagent
                        tool_run = subagent_session.create_child(
                            name=tool_name,
                            run_type="tool",
                            inputs={"input": tool_input} if tool_input else {},
                            start_time=datetime.fromtimestamp(
                                start_time,
                                tz=timezone.utc,
                            ),
                        )
                        tool_run.post()
                        _client_managed_runs[tool_use_id] = tool_run

                except Exception as e:
                    logger.warning(f"Failed to create client-managed tool run: {e}")

        async def receive_response(self) -> AsyncGenerator[Any, None]:
            """Intercept message stream and record chain run activity."""
            messages = super().receive_response()

            # Capture configuration in inputs and metadata
            trace_inputs: dict[str, Any] = {}
            trace_metadata = {}

            # Add prompt to inputs
            if self._prompt:
                trace_inputs["prompt"] = self._prompt

            # Add system_prompt to inputs if available
            if hasattr(self, "options") and self.options:
                if (
                    hasattr(self.options, "system_prompt")
                    and self.options.system_prompt
                ):
                    system_prompt = self.options.system_prompt
                    if isinstance(system_prompt, str):
                        trace_inputs["system"] = system_prompt
                    elif isinstance(system_prompt, dict):
                        # Handle SystemPromptPreset format
                        if system_prompt.get("type") == "preset":
                            preset_text = (
                                f"preset: {system_prompt.get('preset', 'claude_code')}"
                            )
                            if "append" in system_prompt:
                                preset_text += f"\nappend: {system_prompt['append']}"
                            trace_inputs["system"] = preset_text
                        else:
                            trace_inputs["system"] = system_prompt

                # Add other config to metadata
                for attr in ["model", "permission_mode", "max_turns"]:
                    if hasattr(self.options, attr):
                        val = getattr(self.options, attr)
                        if val is not None:
                            trace_metadata[attr] = val

            async with trace(
                name=TRACE_CHAIN_NAME,
                run_type="chain",
                inputs=trace_inputs,
                metadata=trace_metadata,
            ) as run:
                set_parent_run_tree(run)
                tracker = TurnLifecycle(self._start_time)
                collected: list[dict[str, Any]] = []

                # Track subagent sessions by Task tool_use_id
                subagent_sessions: dict[str, Any] = {}

                try:
                    async for msg in messages:
                        msg_type = type(msg).__name__
                        if msg_type == "AssistantMessage":
                            content = tracker.start_llm_run(
                                msg, self._prompt, collected
                            )
                            if content:
                                collected.append(content)

                            # Process tool uses in this AssistantMessage
                            self._handle_assistant_tool_uses(
                                msg,
                                run,
                                subagent_sessions,
                            )
                        elif msg_type == "UserMessage":
                            # Check if this is a subagent message
                            parent_tool_use_id = getattr(
                                msg, "parent_tool_use_id", None
                            )
                            if (
                                parent_tool_use_id
                                and parent_tool_use_id in subagent_sessions
                            ):
                                # Subagent input message - update session
                                subagent_session = subagent_sessions[parent_tool_use_id]
                                if hasattr(msg, "content"):
                                    try:
                                        msg_content = flatten_content_blocks(
                                            msg.content
                                        )
                                        subagent_session.inputs = {
                                            "prompt": msg_content
                                        }
                                    except Exception as e:
                                        logger.warning(
                                            f"Failed to set subagent "
                                            f"session inputs: {e}"
                                        )

                            if hasattr(msg, "content"):
                                collected.append(
                                    {
                                        "content": flatten_content_blocks(msg.content),
                                        "role": "user",
                                    }
                                )
                            tracker.mark_next_start()
                        elif msg_type == "ResultMessage":
                            # Add usage metrics including cost
                            if hasattr(msg, "usage"):
                                usage = extract_usage_from_result_message(msg)
                                # Add total_cost to usage_metadata if available
                                if (
                                    hasattr(msg, "total_cost_usd")
                                    and msg.total_cost_usd is not None
                                ):
                                    usage["total_cost"] = msg.total_cost_usd
                                tracker.add_usage(usage)

                            # Add conversation-level metadata
                            meta = {
                                k: v
                                for k, v in {
                                    "num_turns": getattr(msg, "num_turns", None),
                                    "session_id": getattr(msg, "session_id", None),
                                    "duration_ms": getattr(msg, "duration_ms", None),
                                    "duration_api_ms": getattr(
                                        msg, "duration_api_ms", None
                                    ),
                                    "is_error": getattr(msg, "is_error", None),
                                }.items()
                                if v is not None
                            }
                            if meta:
                                run.metadata.update(meta)

                        yield msg
                    run.end(outputs=collected[-1] if collected else None)
                except Exception:
                    logger.exception("Error while tracing Claude Agent stream")
                finally:
                    tracker.close()
                    clear_parent_run_tree()
                    clear_active_tool_runs()

        async def __aenter__(self) -> "TracedClaudeSDKClient":
            await super().__aenter__()
            return self

        async def __aexit__(self, *args: Any) -> None:
            await super().__aexit__(*args)

    return TracedClaudeSDKClient
