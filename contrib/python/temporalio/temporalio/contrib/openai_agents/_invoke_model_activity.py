"""A temporal activity that invokes a LLM model.

Implements mapping of OpenAI datastructures to Pydantic friendly types.
"""

import enum
from collections.abc import Collection
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NoReturn

from agents import (
    AgentOutputSchemaBase,
    CodeInterpreterTool,
    FileSearchTool,
    FunctionTool,
    Handoff,
    HostedMCPTool,
    ImageGenerationTool,
    ModelProvider,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    OpenAIProvider,
    RunContextWrapper,
    Tool,
    TResponseInputItem,
    UserError,
    WebSearchTool,
)
from agents.items import TResponseStreamEvent
from agents.tool import (
    ApplyPatchTool,
    CustomTool,
    LocalShellTool,
    ShellTool,
    ShellToolEnvironment,
    ToolSearchTool,
)
from openai import (
    APIStatusError,
    AsyncOpenAI,
)
from openai.types.responses import CustomToolParam
from openai.types.responses.tool_param import Mcp
from typing_extensions import Required, TypedDict

from temporalio import activity
from temporalio.contrib.openai_agents._heartbeat_decorator import auto_heartbeater
from temporalio.contrib.openai_agents._temporal_worker_env_ref import (
    AllowAllWorkerEnvVars,
    _WorkerEnvRefResolver,
)
from temporalio.contrib.workflow_streams import WorkflowStreamClient
from temporalio.exceptions import ApplicationError


@dataclass
class HandoffInput:
    """Data conversion friendly representation of a Handoff. Contains only the fields which are needed by the model
    execution to determine what to handoff to, not the actual handoff invocation, which remains in the workflow context.
    """

    tool_name: str
    tool_description: str
    input_json_schema: dict[str, Any]
    agent_name: str
    strict_json_schema: bool = True


@dataclass
class FunctionToolInput:
    """Data conversion friendly representation of a FunctionTool. Contains only the fields which are needed by the model
    execution to determine what tool to call, not the actual tool invocation, which remains in the workflow context.
    """

    name: str
    description: str
    params_json_schema: dict[str, Any]
    strict_json_schema: bool = True


@dataclass
class HostedMCPToolInput:
    """Data conversion friendly representation of a HostedMCPTool. Contains only the fields which are needed by the model
    execution to determine what tool to call, not the actual tool invocation, which remains in the workflow context.
    """

    tool_config: Mcp


@dataclass
class ShellToolInput:
    """Data conversion friendly representation of a ShellTool. Contains only the fields which are needed by the model
    execution to determine what tool to call, not the actual tool invocation, which remains in the workflow context.
    """

    name: str = "shell"
    environment: ShellToolEnvironment | None = None


class _NoopApplyPatchEditor:
    """Satisfies the ApplyPatchEditor protocol for tool reconstruction during model calls."""

    def create_file(self, operation: Any) -> None:  # type: ignore[reportUnusedParameter]
        return None

    def update_file(self, operation: Any) -> None:  # type: ignore[reportUnusedParameter]
        return None

    def delete_file(self, operation: Any) -> None:  # type: ignore[reportUnusedParameter]
        return None


@dataclass
class ApplyPatchToolInput:
    """Data conversion friendly representation of an ApplyPatchTool."""

    name: str = "apply_patch"


@dataclass
class CustomToolInput:
    """Data conversion friendly representation of a CustomTool. Contains only the fields which are needed by the model
    execution to determine what tool to call, not the actual tool invocation, which remains in the workflow context.
    """

    tool_config: CustomToolParam


ToolInput = (
    FunctionToolInput
    | FileSearchTool
    | WebSearchTool
    | ImageGenerationTool
    | CodeInterpreterTool
    | HostedMCPToolInput
    | ShellToolInput
    | LocalShellTool
    | ApplyPatchToolInput
    | CustomToolInput
    | ToolSearchTool
)


@dataclass
class AgentOutputSchemaInput(AgentOutputSchemaBase):
    """Data conversion friendly representation of AgentOutputSchema."""

    output_type_name: str | None
    is_wrapped: bool
    output_schema: dict[str, Any] | None
    strict_json_schema: bool

    def is_plain_text(self) -> bool:
        """Whether the output type is plain text (versus a JSON object)."""
        return self.output_type_name is None or self.output_type_name == "str"

    def is_strict_json_schema(self) -> bool:
        """Whether the JSON schema is in strict mode."""
        return self.strict_json_schema

    def json_schema(self) -> dict[str, Any]:
        """The JSON schema of the output type."""
        if self.is_plain_text():
            raise UserError("Output type is plain text, so no JSON schema is available")
        if self.output_schema is None:
            raise UserError("Output schema is not defined")
        return self.output_schema

    def validate_json(self, json_str: str) -> Any:
        """Validate the JSON string against the schema."""
        raise NotImplementedError()

    def name(self) -> str:
        """Get the name of the output type."""
        if self.output_type_name is None:
            raise ValueError("output_type_name is None")
        return self.output_type_name


class ModelTracingInput(enum.IntEnum):
    """Conversion friendly representation of ModelTracing.

    Needed as ModelTracing is enum.Enum instead of IntEnum
    """

    DISABLED = 0
    ENABLED = 1
    ENABLED_WITHOUT_DATA = 2


class ActivityModelInput(TypedDict, total=False):
    """Input for the invoke_model_activity activity."""

    model_name: str | None
    system_instructions: str | None
    input: Required[str | list[TResponseInputItem]]
    model_settings: Required[ModelSettings]
    tools: list[ToolInput]
    output_schema: AgentOutputSchemaInput | None
    handoffs: list[HandoffInput]
    tracing: Required[ModelTracingInput]
    previous_response_id: str | None
    conversation_id: str | None
    prompt: Any | None


class StreamingActivityModelInput(ActivityModelInput, total=False):
    """Input for the invoke_model_activity_streaming activity.

    Adds the streaming-only fields on top of :class:`ActivityModelInput`.
    """

    streaming_topic: Required[str]
    streaming_batch_interval: timedelta


async def _empty_on_invoke_tool(_ctx: RunContextWrapper[Any], _input: str) -> str:
    return ""


async def _empty_on_invoke_handoff(_ctx: RunContextWrapper[Any], _input: str) -> Any:
    return None


async def _noop_shell_executor(*_a: Any, **_kw: Any) -> str:
    return ""


def _build_tool(tool: ToolInput, env_refs: _WorkerEnvRefResolver) -> Tool:
    """Reconstruct a Tool from its data-conversion-friendly input form."""
    if isinstance(
        tool,
        (
            FileSearchTool,
            WebSearchTool,
            ImageGenerationTool,
            LocalShellTool,
            ToolSearchTool,
        ),
    ):
        return tool
    elif isinstance(tool, CodeInterpreterTool):
        return CodeInterpreterTool(
            tool_config=env_refs.resolve_code_interpreter_tool_config(tool.tool_config)
        )
    elif isinstance(tool, ShellToolInput):
        environment = env_refs.resolve_shell_tool_environment(tool.environment)
        # Only a local environment takes an executor.
        return ShellTool(
            name=tool.name,
            environment=environment,
            executor=_noop_shell_executor if environment["type"] == "local" else None,
        )
    elif isinstance(tool, ApplyPatchToolInput):
        return ApplyPatchTool(name=tool.name, editor=_NoopApplyPatchEditor())
    elif isinstance(tool, HostedMCPToolInput):
        return HostedMCPTool(
            tool_config=env_refs.resolve_mcp_tool_config(tool.tool_config)
        )
    elif isinstance(tool, CustomToolInput):
        return CustomTool(
            name=tool.tool_config["name"],
            description=tool.tool_config.get("description", ""),
            on_invoke_tool=_empty_on_invoke_tool,
            format=tool.tool_config.get("format"),
            defer_loading=tool.tool_config.get("defer_loading", False),
        )
    elif isinstance(tool, FunctionToolInput):
        return FunctionTool(
            name=tool.name,
            description=tool.description,
            params_json_schema=tool.params_json_schema,
            on_invoke_tool=_empty_on_invoke_tool,
            strict_json_schema=tool.strict_json_schema,
        )
    else:
        raise UserError(f"Unknown tool type: {tool.name}")  # type:ignore[reportUnreachable]


def _build_tools_and_handoffs(
    input: ActivityModelInput,
    env_refs: _WorkerEnvRefResolver,
) -> tuple[list[Tool], list[Handoff[Any, Any]]]:
    tools = [_build_tool(x, env_refs) for x in input.get("tools", [])]
    handoffs: list[Handoff[Any, Any]] = [
        Handoff(
            tool_name=x.tool_name,
            tool_description=x.tool_description,
            input_json_schema=x.input_json_schema,
            agent_name=x.agent_name,
            strict_json_schema=x.strict_json_schema,
            on_invoke_handoff=_empty_on_invoke_handoff,
        )
        for x in input.get("handoffs", [])
    ]
    return tools, handoffs


def _raise_for_openai_status(e: APIStatusError) -> NoReturn:
    """Translate an OpenAI APIStatusError into the right retry posture."""
    retry_after: timedelta | None = None
    retry_after_ms_header = e.response.headers.get("retry-after-ms")
    if retry_after_ms_header is not None:
        retry_after = timedelta(milliseconds=float(retry_after_ms_header))

    if retry_after is None:
        retry_after_header = e.response.headers.get("retry-after")
        if retry_after_header is not None:
            retry_after = timedelta(seconds=float(retry_after_header))

    should_retry_header = e.response.headers.get("x-should-retry")
    if should_retry_header == "true":
        raise e
    if should_retry_header == "false":
        raise ApplicationError(
            "Non retryable OpenAI error",
            non_retryable=True,
            next_retry_delay=retry_after,
        ) from e

    # Retry on 408 (Request Timeout), 409 (Conflict / often transient
    # state mismatch), 429 (Too Many Requests / rate-limited), and any
    # 5xx (server-side errors). All other 4xx codes are caller errors
    # that won't recover on retry.
    retryable = (
        e.response.status_code in [408, 409, 429] or e.response.status_code >= 500
    )
    raise ApplicationError(
        f"{'Retryable' if retryable else 'Non retryable'} OpenAI status code: "
        f"{e.response.status_code}",
        non_retryable=not retryable,
        next_retry_delay=retry_after,
    ) from e


class ModelActivity:
    """Class wrapper for model invocation activities to allow model customization. By default, we use an OpenAIProvider with retries disabled.
    Disabling retries in your model of choice is recommended to allow activity retries to define the retry model.
    """

    def __init__(
        self,
        model_provider: ModelProvider | None = None,
        resolvable_worker_env_vars: Collection[str] | AllowAllWorkerEnvVars = (),
    ):
        """Initialize the activity with a model provider."""
        self._model_provider = model_provider or OpenAIProvider(
            openai_client=AsyncOpenAI(max_retries=0)
        )
        self._env_refs = _WorkerEnvRefResolver(resolvable_worker_env_vars)

    @activity.defn
    @auto_heartbeater
    async def invoke_model_activity(self, input: ActivityModelInput) -> ModelResponse:
        """Activity that invokes a model with the given input."""
        model = self._model_provider.get_model(input.get("model_name"))
        tools, handoffs = _build_tools_and_handoffs(input, self._env_refs)

        try:
            return await model.get_response(
                system_instructions=input.get("system_instructions"),
                input=input["input"],
                model_settings=input["model_settings"],
                tools=tools,
                output_schema=input.get("output_schema"),
                handoffs=handoffs,
                tracing=ModelTracing(input["tracing"]),
                previous_response_id=input.get("previous_response_id"),
                conversation_id=input.get("conversation_id"),
                prompt=input.get("prompt"),
            )
        except APIStatusError as e:
            _raise_for_openai_status(e)

    @activity.defn
    @auto_heartbeater
    async def invoke_model_activity_streaming(
        self, input: StreamingActivityModelInput
    ) -> list[TResponseStreamEvent]:
        """Streaming-aware model activity.

        .. warning::
            Streaming support is experimental and may change in future
            versions.

        Calls ``model.stream_response()`` and returns the collected list
        of native OpenAI stream events. The workflow's
        ``Model.stream_response`` stub yields these to the agents
        framework, which builds the final ``ModelResponse`` from the
        terminal ``ResponseCompletedEvent``.

        Each event is also published to the workflow's stream on
        ``streaming_topic`` so external consumers (UIs, tracing,
        etc.) can observe events as they arrive.

        Heartbeats run on a background task via ``auto_heartbeater`` so
        long initial-token latency or long pauses between chunks do not
        trip ``heartbeat_timeout``.
        """
        model = self._model_provider.get_model(input.get("model_name"))
        tools, handoffs = _build_tools_and_handoffs(input, self._env_refs)

        topic = input["streaming_topic"]
        batch_interval = input.get(
            "streaming_batch_interval", timedelta(milliseconds=100)
        )
        events: list[TResponseStreamEvent] = []

        stream = WorkflowStreamClient.from_within_activity(
            batch_interval=batch_interval
        )
        # TResponseStreamEvent is a typing.Annotated[Union[...]] — a typing
        # special form, not a class — so it cannot be passed as type[T].
        # Leave the topic untyped (default Any); subscribers that want
        # typed decode can pass result_type=TResponseStreamEvent on
        # their own subscribe call.
        events_topic = stream.topic(topic)
        async with stream:
            try:
                async for event in model.stream_response(
                    system_instructions=input.get("system_instructions"),
                    input=input["input"],
                    model_settings=input["model_settings"],
                    tools=tools,
                    output_schema=input.get("output_schema"),
                    handoffs=handoffs,
                    tracing=ModelTracing(input["tracing"]),
                    previous_response_id=input.get("previous_response_id"),
                    conversation_id=input.get("conversation_id"),
                    prompt=input.get("prompt"),
                ):
                    # OpenAI models set defer_build=True, so an event's pydantic
                    # schema may still be an unbuilt placeholder.
                    type(event).model_rebuild()
                    events.append(event)
                    events_topic.publish(event)
            except APIStatusError as e:
                _raise_for_openai_status(e)

        return events
