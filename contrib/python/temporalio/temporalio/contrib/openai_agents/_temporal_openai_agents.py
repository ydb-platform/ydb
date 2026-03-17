"""Initialize Temporal OpenAI Agents overrides."""

from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from typing import AsyncIterator, Callable, Optional, Union

from agents import (
    AgentOutputSchemaBase,
    Handoff,
    Model,
    ModelProvider,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    Tool,
    TResponseInputItem,
    set_trace_provider,
)
from agents.items import TResponseStreamEvent
from agents.run import get_default_agent_runner, set_default_agent_runner
from agents.tracing import get_trace_provider
from agents.tracing.provider import DefaultTraceProvider
from openai.types.responses import ResponsePromptParam

import temporalio.client
import temporalio.worker
from temporalio.client import ClientConfig, Plugin
from temporalio.contrib.openai_agents._invoke_model_activity import ModelActivity
from temporalio.contrib.openai_agents._model_parameters import ModelActivityParameters
from temporalio.contrib.openai_agents._openai_runner import TemporalOpenAIRunner
from temporalio.contrib.openai_agents._temporal_trace_provider import (
    TemporalTraceProvider,
)
from temporalio.contrib.openai_agents._trace_interceptor import (
    OpenAIAgentsTracingInterceptor,
)
from temporalio.contrib.pydantic import (
    PydanticPayloadConverter,
    ToJsonOptions,
)
from temporalio.converter import (
    DataConverter,
)
from temporalio.worker import (
    Replayer,
    ReplayerConfig,
    Worker,
    WorkerConfig,
    WorkflowReplayResult,
)


@contextmanager
def set_open_ai_agent_temporal_overrides(
    model_params: ModelActivityParameters,
    auto_close_tracing_in_workflows: bool = False,
):
    """Configure Temporal-specific overrides for OpenAI agents.

    .. warning::
        This API is experimental and may change in future versions.
        Use with caution in production environments. Future versions may wrap the worker directly
        instead of requiring this context manager.

    This context manager sets up the necessary Temporal-specific runners and trace providers
    for running OpenAI agents within Temporal workflows. It should be called in the main
    entry point of your application before initializing the Temporal client and worker.

    The context manager handles:
    1. Setting up a Temporal-specific runner for OpenAI agents
    2. Configuring a Temporal-aware trace provider
    3. Restoring previous settings when the context exits

    Args:
        model_params: Configuration parameters for Temporal activity execution of model calls.
        auto_close_tracing_in_workflows: If set to true, close tracing spans immediately.

    Returns:
        A context manager that yields the configured TemporalTraceProvider.
    """
    previous_runner = get_default_agent_runner()
    previous_trace_provider = get_trace_provider()
    provider = TemporalTraceProvider(
        auto_close_in_workflows=auto_close_tracing_in_workflows
    )

    try:
        set_default_agent_runner(TemporalOpenAIRunner(model_params))
        set_trace_provider(provider)
        yield provider
    finally:
        set_default_agent_runner(previous_runner)
        set_trace_provider(previous_trace_provider or DefaultTraceProvider())


class TestModelProvider(ModelProvider):
    """Test model provider which simply returns the given module."""

    def __init__(self, model: Model):
        """Initialize a test model provider with a model."""
        self._model = model

    def get_model(self, model_name: Union[str, None]) -> Model:
        """Get a model from the model provider."""
        return self._model


class TestModel(Model):
    """Test model for use mocking model responses."""

    def __init__(self, fn: Callable[[], ModelResponse]) -> None:
        """Initialize a test model with a callable."""
        self.fn = fn

    async def get_response(
        self,
        system_instructions: Union[str, None],
        input: Union[str, list[TResponseInputItem]],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: Union[AgentOutputSchemaBase, None],
        handoffs: list[Handoff],
        tracing: ModelTracing,
        *,
        previous_response_id: Union[str, None],
        prompt: Union[ResponsePromptParam, None] = None,
    ) -> ModelResponse:
        """Get a response from the model."""
        return self.fn()

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
        prompt: Optional[ResponsePromptParam],
    ) -> AsyncIterator[TResponseStreamEvent]:
        """Get a streamed response from the model. Unimplemented."""
        raise NotImplementedError()


class _OpenAIPayloadConverter(PydanticPayloadConverter):
    def __init__(self) -> None:
        super().__init__(ToJsonOptions(exclude_unset=True))


class OpenAIAgentsPlugin(temporalio.client.Plugin, temporalio.worker.Plugin):
    """Temporal plugin for integrating OpenAI agents with Temporal workflows.

    .. warning::
        This class is experimental and may change in future versions.
        Use with caution in production environments.

    This plugin provides seamless integration between the OpenAI Agents SDK and
    Temporal workflows. It automatically configures the necessary interceptors,
    activities, and data converters to enable OpenAI agents to run within
    Temporal workflows with proper tracing and model execution.

    The plugin:
    1. Configures the Pydantic data converter for type-safe serialization
    2. Sets up tracing interceptors for OpenAI agent interactions
    3. Registers model execution activities
    4. Manages the OpenAI agent runtime overrides during worker execution

    Args:
        model_params: Configuration parameters for Temporal activity execution
            of model calls. If None, default parameters will be used.
        model_provider: Optional model provider for custom model implementations.
            Useful for testing or custom model integrations.

    Example:
        >>> from temporalio.client import Client
        >>> from temporalio.worker import Worker
        >>> from temporalio.contrib.openai_agents import OpenAIAgentsPlugin, ModelActivityParameters
        >>> from datetime import timedelta
        >>>
        >>> # Configure model parameters
        >>> model_params = ModelActivityParameters(
        ...     start_to_close_timeout=timedelta(seconds=30),
        ...     retry_policy=RetryPolicy(maximum_attempts=3)
        ... )
        >>>
        >>> # Create plugin
        >>> plugin = OpenAIAgentsPlugin(model_params=model_params)
        >>>
        >>> # Use with client and worker
        >>> client = await Client.connect(
        ...     "localhost:7233",
        ...     plugins=[plugin]
        ... )
        >>> worker = Worker(
        ...     client,
        ...     task_queue="my-task-queue",
        ...     workflows=[MyWorkflow],
        ... )
    """

    def __init__(
        self,
        model_params: Optional[ModelActivityParameters] = None,
        model_provider: Optional[ModelProvider] = None,
    ) -> None:
        """Initialize the OpenAI agents plugin.

        Args:
            model_params: Configuration parameters for Temporal activity execution
                of model calls. If None, default parameters will be used.
            model_provider: Optional model provider for custom model implementations.
                Useful for testing or custom model integrations.
        """
        if model_params is None:
            model_params = ModelActivityParameters()

        # For the default provider, we provide a default start_to_close_timeout of 60 seconds.
        # Other providers will need to define their own.
        if (
            model_params.start_to_close_timeout is None
            and model_params.schedule_to_close_timeout is None
        ):
            if model_provider is None:
                model_params.start_to_close_timeout = timedelta(seconds=60)
            else:
                raise ValueError(
                    "When configuring a custom provider, the model activity must have start_to_close_timeout or schedule_to_close_timeout"
                )

        self._model_params = model_params
        self._model_provider = model_provider

    def init_client_plugin(self, next: temporalio.client.Plugin) -> None:
        """Set the next client plugin"""
        self.next_client_plugin = next

    async def connect_service_client(
        self, config: temporalio.service.ConnectConfig
    ) -> temporalio.service.ServiceClient:
        """No modifications to service client"""
        return await self.next_client_plugin.connect_service_client(config)

    def init_worker_plugin(self, next: temporalio.worker.Plugin) -> None:
        """Set the next worker plugin"""
        self.next_worker_plugin = next

    def configure_client(self, config: ClientConfig) -> ClientConfig:
        """Configure the Temporal client for OpenAI agents integration.

        This method sets up the Pydantic data converter to enable proper
        serialization of OpenAI agent objects and responses.

        Args:
            config: The client configuration to modify.

        Returns:
            The modified client configuration.
        """
        config["data_converter"] = DataConverter(
            payload_converter_class=_OpenAIPayloadConverter
        )
        return self.next_client_plugin.configure_client(config)

    def configure_worker(self, config: WorkerConfig) -> WorkerConfig:
        """Configure the Temporal worker for OpenAI agents integration.

        This method adds the necessary interceptors and activities for OpenAI
        agent execution:
        - Adds tracing interceptors for OpenAI agent interactions
        - Registers model execution activities

        Args:
            config: The worker configuration to modify.

        Returns:
            The modified worker configuration.
        """
        config["interceptors"] = list(config.get("interceptors") or []) + [
            OpenAIAgentsTracingInterceptor()
        ]
        config["activities"] = list(config.get("activities") or []) + [
            ModelActivity(self._model_provider).invoke_model_activity
        ]
        return self.next_worker_plugin.configure_worker(config)

    async def run_worker(self, worker: Worker) -> None:
        """Run the worker with OpenAI agents temporal overrides.

        This method sets up the necessary runtime overrides for OpenAI agents
        to work within the Temporal worker context, including custom runners
        and trace providers.

        Args:
            worker: The worker instance to run.
        """
        with set_open_ai_agent_temporal_overrides(self._model_params):
            await self.next_worker_plugin.run_worker(worker)

    def configure_replayer(self, config: ReplayerConfig) -> ReplayerConfig:
        """Configure the replayer for OpenAI Agents."""
        config["interceptors"] = list(config.get("interceptors") or []) + [
            OpenAIAgentsTracingInterceptor()
        ]
        config["data_converter"] = DataConverter(
            payload_converter_class=_OpenAIPayloadConverter
        )
        return config

    @asynccontextmanager
    async def run_replayer(
        self,
        replayer: Replayer,
        histories: AsyncIterator[temporalio.client.WorkflowHistory],
    ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        """Set the OpenAI Overrides during replay"""
        with set_open_ai_agent_temporal_overrides(self._model_params):
            async with self.next_worker_plugin.run_replayer(
                replayer, histories
            ) as results:
                yield results
