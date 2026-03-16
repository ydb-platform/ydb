from __future__ import annotations

import logging
from typing import Optional

from temporalio import workflow
from temporalio.contrib.openai_agents._model_parameters import ModelActivityParameters

logger = logging.getLogger(__name__)

from typing import Any, AsyncIterator, Union, cast

from agents import (
    Agent,
    AgentOutputSchema,
    AgentOutputSchemaBase,
    CodeInterpreterTool,
    FileSearchTool,
    FunctionTool,
    Handoff,
    HostedMCPTool,
    ImageGenerationTool,
    Model,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    Tool,
    TResponseInputItem,
    WebSearchTool,
)
from agents.items import TResponseStreamEvent
from openai.types.responses.response_prompt_param import ResponsePromptParam

from temporalio.contrib.openai_agents._invoke_model_activity import (
    ActivityModelInput,
    AgentOutputSchemaInput,
    FunctionToolInput,
    HandoffInput,
    HostedMCPToolInput,
    ModelActivity,
    ModelTracingInput,
    ToolInput,
)


class _TemporalModelStub(Model):
    """A stub that allows invoking models as Temporal activities."""

    def __init__(
        self,
        model_name: Optional[str],
        *,
        model_params: ModelActivityParameters,
        agent: Optional[Agent[Any]],
    ) -> None:
        self.model_name = model_name
        self.model_params = model_params
        self.agent = agent

    async def get_response(
        self,
        system_instructions: Optional[str],
        input: Union[str, list[TResponseInputItem]],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: Optional[AgentOutputSchemaBase],
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: Optional[str],
        prompt: Optional[ResponsePromptParam],
    ) -> ModelResponse:
        def make_tool_info(tool: Tool) -> ToolInput:
            if isinstance(
                tool,
                (
                    FileSearchTool,
                    WebSearchTool,
                    ImageGenerationTool,
                    CodeInterpreterTool,
                ),
            ):
                return tool
            elif isinstance(tool, HostedMCPTool):
                return HostedMCPToolInput(tool_config=tool.tool_config)
            elif isinstance(tool, FunctionTool):
                return FunctionToolInput(
                    name=tool.name,
                    description=tool.description,
                    params_json_schema=tool.params_json_schema,
                    strict_json_schema=tool.strict_json_schema,
                )
            else:
                raise ValueError(f"Unsupported tool type: {tool.name}")

        tool_infos = [make_tool_info(x) for x in tools]
        handoff_infos = [
            HandoffInput(
                tool_name=x.tool_name,
                tool_description=x.tool_description,
                input_json_schema=x.input_json_schema,
                agent_name=x.agent_name,
                strict_json_schema=x.strict_json_schema,
            )
            for x in handoffs
        ]
        if output_schema is not None and not isinstance(
            output_schema, AgentOutputSchema
        ):
            raise TypeError(
                f"Only AgentOutputSchema is supported by Temporal Model, got {type(output_schema).__name__}"
            )
        agent_output_schema = output_schema
        output_schema_input = (
            None
            if agent_output_schema is None
            else AgentOutputSchemaInput(
                output_type_name=agent_output_schema.name(),
                is_wrapped=agent_output_schema._is_wrapped,
                output_schema=agent_output_schema.json_schema()
                if not agent_output_schema.is_plain_text()
                else None,
                strict_json_schema=agent_output_schema.is_strict_json_schema(),
            )
        )

        activity_input = ActivityModelInput(
            model_name=self.model_name,
            system_instructions=system_instructions,
            input=input,
            model_settings=model_settings,
            tools=tool_infos,
            output_schema=output_schema_input,
            handoffs=handoff_infos,
            tracing=ModelTracingInput(tracing.value),
            previous_response_id=previous_response_id,
            prompt=prompt,
        )

        if self.model_params.summary_override:
            summary = (
                self.model_params.summary_override
                if isinstance(self.model_params.summary_override, str)
                else (
                    self.model_params.summary_override.provide(
                        self.agent, system_instructions, input
                    )
                )
            )
        elif self.agent:
            summary = self.agent.name
        else:
            summary = None

        return await workflow.execute_activity_method(
            ModelActivity.invoke_model_activity,
            activity_input,
            summary=summary,
            task_queue=self.model_params.task_queue,
            schedule_to_close_timeout=self.model_params.schedule_to_close_timeout,
            schedule_to_start_timeout=self.model_params.schedule_to_start_timeout,
            start_to_close_timeout=self.model_params.start_to_close_timeout,
            heartbeat_timeout=self.model_params.heartbeat_timeout,
            retry_policy=self.model_params.retry_policy,
            cancellation_type=self.model_params.cancellation_type,
            versioning_intent=self.model_params.versioning_intent,
            priority=self.model_params.priority,
        )

    def stream_response(
        self,
        system_instructions: Optional[str],
        input: Union[str, list[TResponseInputItem]],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: Optional[AgentOutputSchemaBase],
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: Optional[str],
        prompt: ResponsePromptParam | None,
    ) -> AsyncIterator[TResponseStreamEvent]:
        raise NotImplementedError("Temporal model doesn't support streams yet")


def _extract_summary(input: Union[str, list[TResponseInputItem]]) -> str:
    ### Activity summary shown in the UI
    try:
        max_size = 100
        if isinstance(input, str):
            return input[:max_size]
        elif isinstance(input, list):
            # Find all message inputs, which are reasonably summarizable
            messages: list[TResponseInputItem] = [
                item for item in input if item.get("type", "message") == "message"
            ]
            if not messages:
                return ""

            content: Any = messages[-1].get("content", "")

            # In the case of multiple contents, take the last one
            if isinstance(content, list):
                if not content:
                    return ""
                content = content[-1]

            # Take the text field from the content if present
            if isinstance(content, dict) and content.get("text") is not None:
                content = content.get("text")
            return str(content)[:max_size]
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
    return ""
