import dataclasses
from collections.abc import AsyncIterator, Awaitable
from typing import Any, Callable

from agents import (
    Agent,
    AgentsException,
    Handoff,
    RunConfig,
    RunContextWrapper,
    RunResult,
    RunResultStreaming,
    RunState,
    SQLiteSession,
    TContext,
    TResponseInputItem,
)
from agents.run import DEFAULT_AGENT_RUNNER, AgentRunner, RunOptions
from agents.sandbox import SandboxAgent
from typing_extensions import Unpack

from temporalio import workflow
from temporalio.contrib.openai_agents._errors import AgentsWorkflowError
from temporalio.contrib.openai_agents._model_parameters import ModelActivityParameters
from temporalio.contrib.openai_agents._temporal_model_stub import _TemporalModelStub
from temporalio.contrib.openai_agents.sandbox._temporal_sandbox_client import (
    TemporalSandboxClient,
)


# Recursively replace models in all agents
def _convert_agent(
    model_params: ModelActivityParameters,
    agent: Agent[Any],
    seen: dict[int, Agent] | None,
) -> Agent[Any]:
    if seen is None:
        seen = dict()

    # Short circuit if this model was already seen to prevent looping from circular handoffs
    if id(agent) in seen:
        return seen[id(agent)]

    # This agent has already been processed in some other run
    if isinstance(agent.model, _TemporalModelStub):
        return agent

    # Save the new version of the agent so that we can replace loops
    new_agent = dataclasses.replace(agent)
    seen[id(agent)] = new_agent

    name = _model_name(agent)

    new_handoffs: list[Agent | Handoff] = []
    for handoff in agent.handoffs:
        if isinstance(handoff, Agent):
            new_handoffs.append(_convert_agent(model_params, handoff, seen))
        elif isinstance(handoff, Handoff):
            original_invoke = handoff.on_invoke_handoff

            # Use default parameter to capture original_invoke by value, not reference
            async def on_invoke(
                context: RunContextWrapper[Any],
                args: str,
                invoke_func: Callable[
                    [RunContextWrapper[Any], str], Awaitable[Any]
                ] = original_invoke,
            ) -> Agent:
                handoff_agent = await invoke_func(context, args)
                return _convert_agent(model_params, handoff_agent, seen)

            new_handoffs.append(
                dataclasses.replace(handoff, on_invoke_handoff=on_invoke)
            )
        else:
            raise TypeError(f"Unknown handoff type: {type(handoff)}")

    new_agent.model = _TemporalModelStub(
        model_name=name,
        model_params=model_params,
        agent=agent,
    )
    new_agent.handoffs = new_handoffs
    return new_agent


def _has_sandbox_agent(agent: Agent[Any], seen: set[int] | None = None) -> bool:
    """Check if any agent in the graph (following direct Agent handoffs) is a SandboxAgent."""
    if seen is None:
        seen = set()
    if id(agent) in seen:
        return False
    seen.add(id(agent))
    if isinstance(agent, SandboxAgent):
        return True
    for handoff in agent.handoffs:
        if isinstance(handoff, Agent) and _has_sandbox_agent(handoff, seen):
            return True
    return False


def _coerce_run_config(value: object) -> RunConfig:
    """openai-agents >= 0.19 also accepts a plain dict for ``run_config``.

    This function normalizes to a RunConfig instance.
    """
    if isinstance(value, RunConfig):
        return value
    if not isinstance(value, dict):
        raise TypeError(
            f"run_config must be a RunConfig instance or a dict, got {type(value).__name__}"
        )
    field_names = {
        config_field.name
        for config_field in dataclasses.fields(RunConfig)
        if config_field.init
    }
    unknown_fields = sorted(str(name) for name in value if name not in field_names)
    if unknown_fields:
        raise TypeError(f"Unknown run_config settings: {', '.join(unknown_fields)}")
    return RunConfig(**value)


class TemporalOpenAIRunner(AgentRunner):
    """Temporal Runner for OpenAI agents.

    Forwards model calls to a Temporal activity.

    """

    def __init__(
        self,
        model_params: ModelActivityParameters,
    ) -> None:
        """Initialize the Temporal OpenAI Runner."""
        self._runner = DEFAULT_AGENT_RUNNER or AgentRunner()
        self.model_params = model_params

    def _prepare_workflow_run(
        self,
        starting_agent: Agent[TContext],
        kwargs: RunOptions[TContext],
    ) -> Agent[Any]:
        """Workflow-only validation and ``kwargs`` rewrite shared by ``run()`` and ``run_streamed()``."""
        for t in starting_agent.tools:
            if callable(t):
                raise ValueError(
                    "Provided tool is not a tool type. If using an activity, make sure to wrap it with openai_agents.workflow.activity_as_tool."
                )

        if starting_agent.mcp_servers:
            from temporalio.contrib.openai_agents._mcp import (
                _StatefulMCPServerReference,
                _StatelessMCPServerReference,
            )

            for s in starting_agent.mcp_servers:
                if not isinstance(
                    s,
                    (
                        _StatelessMCPServerReference,
                        _StatefulMCPServerReference,
                    ),
                ):
                    raise ValueError(
                        f"Unknown mcp_server type {type(s)} may not work durably."
                    )

        if isinstance(kwargs.get("session"), SQLiteSession):
            raise ValueError("Temporal workflows don't support SQLite sessions.")

        run_config = kwargs.get("run_config")
        run_config = (
            RunConfig() if run_config is None else _coerce_run_config(run_config)
        )

        if run_config.model and not isinstance(run_config.model, _TemporalModelStub):
            if not isinstance(run_config.model, str):
                raise ValueError(
                    "Temporal workflows require a model name to be a string in the run config."
                )
            run_config = dataclasses.replace(
                run_config,
                model=_TemporalModelStub(
                    run_config.model, model_params=self.model_params, agent=None
                ),
            )

        # run_config.sandbox is global for the entire run — configure it if any agent needs it.
        if _has_sandbox_agent(starting_agent) or run_config.sandbox:
            if run_config.sandbox is None:
                raise ValueError(
                    "A SandboxAgent was provided but run_config.sandbox is not configured. "
                    "You must set run_config.sandbox to a SandboxRunConfig. "
                    "For example:\n"
                    "  from temporalio.contrib.openai_agents.workflow import temporal_sandbox_client\n"
                    "  run_config = RunConfig(sandbox=SandboxRunConfig(client=temporal_sandbox_client('my-backend')))"
                )
            elif run_config.sandbox.session is not None:
                raise AgentsWorkflowError(
                    "run_config.sandbox.session is not supported by the Temporal OpenAI Agents "
                    "plugin. A live sandbox session is not a durable construct in a workflow. "
                    "Pass run_config.sandbox.client=temporal_sandbox_client(name) instead."
                )
            elif run_config.sandbox.client is None:
                raise ValueError(
                    "run_config.sandbox.client must be set to a temporal sandbox client. "
                    "Use temporalio.contrib.openai_agents.workflow.temporal_sandbox_client(name) "
                    "to create one, where name matches a SandboxClientProvider registered on the plugin."
                )
            elif not isinstance(run_config.sandbox.client, TemporalSandboxClient):
                raise ValueError(
                    "run_config.sandbox.client must be created via "
                    "temporalio.contrib.openai_agents.workflow.temporal_sandbox_client(name). "
                    "Do not pass a raw sandbox client directly."
                )

        kwargs["run_config"] = run_config
        return _convert_agent(self.model_params, starting_agent, None)

    async def run(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem] | RunState[TContext],
        **kwargs: Unpack[RunOptions[TContext]],
    ) -> RunResult:
        """Run the agent in a Temporal workflow."""
        if not workflow.in_workflow():
            return await self._runner.run(
                starting_agent,
                input,
                **kwargs,
            )

        converted_agent = self._prepare_workflow_run(starting_agent, kwargs)

        try:
            return await self._runner.run(
                starting_agent=converted_agent,
                input=input,
                **kwargs,
            )
        except AgentsException as e:
            # In order for workflow failures to properly fail the workflow, we need to rewrap them in
            # a Temporal error
            if e.__cause__ and workflow.is_failure_exception(e.__cause__):
                reraise = AgentsWorkflowError(
                    f"Workflow failure exception in Agents Framework: {e}"
                )
                reraise.__traceback__ = e.__traceback__
                raise reraise from e.__cause__
            else:
                raise e

    def run_sync(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem] | RunState[TContext],
        **kwargs: Any,
    ) -> RunResult:
        """Run the agent synchronously (not supported in Temporal workflows)."""
        if not workflow.in_workflow():
            return self._runner.run_sync(
                starting_agent,
                input,
                **kwargs,
            )
        raise RuntimeError("Temporal workflows do not support synchronous model calls.")

    def run_streamed(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem] | RunState[TContext],
        **kwargs: Unpack[RunOptions[TContext]],
    ) -> RunResultStreaming:
        """Run the agent with streaming responses.

        .. warning::
            Streaming inside Temporal workflows is experimental and may
            change in future versions.

        Inside a workflow, model calls execute as the streaming model
        activity. The workflow consumes events via
        ``RunResultStreaming.stream_events()`` after each activity
        completes; external clients can subscribe to the configured
        stream topic to receive events as they arrive.
        """
        if not workflow.in_workflow():
            return self._runner.run_streamed(
                starting_agent,
                input,
                **kwargs,
            )

        # Fail-fast before the agents framework starts a background task:
        # validation raised inside ``Model.stream_response`` is otherwise
        # captured into ``RunResultStreaming._stored_exception`` and may
        # be silently dropped if the queue completion sentinel is read
        # before the run_loop_task is observed as done.
        if self.model_params.streaming_topic is None:
            raise AgentsWorkflowError(
                "Runner.run_streamed requires "
                "ModelActivityParameters.streaming_topic to be set."
            )
        if self.model_params.use_local_activity:
            raise AgentsWorkflowError(
                "Runner.run_streamed is incompatible with "
                "use_local_activity (local activities do not support "
                "heartbeats or the workflow stream signal channel)."
            )

        converted_agent = self._prepare_workflow_run(starting_agent, kwargs)

        streamed_result = self._runner.run_streamed(
            starting_agent=converted_agent,
            input=input,
            **kwargs,
        )

        # Mirror the AgentsException -> AgentsWorkflowError rewrap done
        # in run() above. The streaming runner attaches the actual run
        # to ``run_loop_task``; we wrap ``stream_events()`` (rather than
        # the task itself) so the rewrap happens on the consumer's
        # coroutine. Wrapping in a second asyncio task introduces a
        # scheduling gap: ``RunResultStreaming.stream_events()`` reads
        # the queue completion sentinel as soon as the run loop ends,
        # but the wrapper task only resumes its ``await`` after another
        # event-loop tick — between those two points, ``_check_errors``
        # sees no exception and ``_await_task_safely`` later swallows
        # the rewrapped one. Iterating the underlying generator first,
        # then inspecting the finished task on exit, keeps the rewrap
        # race-free without touching ``run_loop_task``.
        original_stream_events = streamed_result.stream_events
        run_loop_task = streamed_result.run_loop_task

        async def _stream_events_with_rewrap() -> AsyncIterator[Any]:
            try:
                async for event in original_stream_events():
                    yield event
            except AgentsException as e:
                _reraise_workflow_failure(e)
                raise
            # The agents framework may have stored the run-loop
            # exception on ``_stored_exception`` (or surfaced it through
            # the iterator) without re-raising it through stream_events.
            # By the time the iterator is exhausted, ``run_loop_task``
            # is done — surface its exception here so a failed run
            # cannot appear successful, applying the workflow-failure
            # rewrap when applicable.
            if run_loop_task is not None and run_loop_task.done():
                exc = run_loop_task.exception()
                if exc is not None:
                    if isinstance(exc, AgentsException):
                        _reraise_workflow_failure(exc)
                    raise exc

        streamed_result.stream_events = _stream_events_with_rewrap  # type: ignore[method-assign]
        return streamed_result


def _reraise_workflow_failure(e: AgentsException) -> None:
    """Rewrap an AgentsException whose cause is a Temporal workflow failure.

    Returns normally when ``e`` is not workflow-failure-bearing so the
    caller can re-raise the original.
    """
    if e.__cause__ and workflow.is_failure_exception(e.__cause__):
        reraise = AgentsWorkflowError(
            f"Workflow failure exception in Agents Framework: {e}"
        )
        reraise.__traceback__ = e.__traceback__
        raise reraise from e.__cause__


def _model_name(agent: Agent[Any]) -> str | None:
    name = agent.model
    if name is not None and not isinstance(name, str):
        raise ValueError(
            "Temporal workflows require a model name to be a string in the agent."
        )
    return name
