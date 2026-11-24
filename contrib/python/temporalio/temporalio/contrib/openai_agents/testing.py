"""Testing utilities for OpenAI agents."""

from collections.abc import AsyncIterator, Callable, Collection, Sequence
from typing import Any

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
    Usage,
)
from agents.items import TResponseOutputItem, TResponseStreamEvent
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)

from temporalio.client import Client
from temporalio.contrib.openai_agents._mcp import (
    StatefulMCPServerProvider,
    StatelessMCPServerProvider,
)
from temporalio.contrib.openai_agents._model_parameters import ModelActivityParameters
from temporalio.contrib.openai_agents._temporal_openai_agents import OpenAIAgentsPlugin
from temporalio.contrib.openai_agents._temporal_worker_env_ref import (
    AllowAllWorkerEnvVars,
    _snapshot_resolvable_env_vars,
)

__all__ = [
    "AgentEnvironment",
    "ResponseBuilders",
    "TestModel",
    "TestModelProvider",
]


class ResponseBuilders:
    """Builders for creating model responses for testing."""

    @staticmethod
    def model_response(output: TResponseOutputItem) -> ModelResponse:
        """Create a ModelResponse with the given output."""
        return ModelResponse(
            output=[output],
            usage=Usage(),
            response_id=None,
        )

    @staticmethod
    def response_output_message(text: str) -> ResponseOutputMessage:
        """Create a ResponseOutputMessage with text content."""
        return ResponseOutputMessage(
            id="",
            content=[
                ResponseOutputText(
                    text=text,
                    annotations=[],
                    type="output_text",
                )
            ],
            role="assistant",
            status="completed",
            type="message",
        )

    @staticmethod
    def tool_call(arguments: str, name: str) -> ModelResponse:
        """Create a ModelResponse with a function tool call."""
        return ResponseBuilders.model_response(
            ResponseFunctionToolCall(
                arguments=arguments,
                call_id="call",
                name=name,
                type="function_call",
                id="id",
                status="completed",
            )
        )

    @staticmethod
    def output_message(text: str) -> ModelResponse:
        """Create a ModelResponse with an output message."""
        return ResponseBuilders.model_response(
            ResponseBuilders.response_output_message(text)
        )


class TestModelProvider(ModelProvider):
    """Test model provider which simply returns the given model."""

    __test__ = False

    def __init__(self, model: Model):
        """Initialize a test model provider with a model."""
        self._model = model

    def get_model(self, model_name: str | None) -> Model:
        """Get a model from the model provider."""
        return self._model


class TestModel(Model):
    """Test model for use mocking model responses."""

    __test__ = False

    def __init__(self, fn: Callable[[], ModelResponse]) -> None:
        """Initialize a test model with a callable."""
        self.fn = fn

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> ModelResponse:
        """Get a response from the mocked model, by calling the callable passed to the constructor."""
        return self.fn()

    def stream_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> AsyncIterator[TResponseStreamEvent]:
        """Get a streamed response from the model. Unimplemented."""
        raise NotImplementedError()

    @staticmethod
    def returning_responses(responses: list[ModelResponse]) -> "TestModel":
        """Create a mock model which sequentially returns responses from a list."""
        i = iter(responses)
        return TestModel(lambda: next(i))


class AgentEnvironment:
    """Testing environment for OpenAI agents with Temporal integration.

    This async context manager provides a convenient way to set up testing environments
    for OpenAI agents with mocked model calls and Temporal integration.

    Example:
        >>> from temporalio.contrib.openai_agents.testing import AgentEnvironment, TestModelProvider, ResponseBuilders
        >>> from temporalio.client import Client
        >>>
        >>> # Create a mock model that returns predefined responses
        >>> mock_model = TestModel.returning_responses([
        ...     ResponseBuilders.output_message("Hello, world!"),
        ...     ResponseBuilders.output_message("How can I help you?")
        ... ])
        >>>
        >>> async with AgentEnvironment(model=mock_model) as env:
        ...     client = env.applied_on_client(client)
        ...     # Use client for testing workflows with mocked model calls
    """

    __test__ = False

    def __init__(
        self,
        model_params: ModelActivityParameters | None = None,
        model_provider: ModelProvider | None = None,
        model: Model | None = None,
        mcp_server_providers: Sequence[
            StatelessMCPServerProvider | StatefulMCPServerProvider
        ] = (),
        register_activities: bool = True,
        add_temporal_spans: bool = True,
        use_otel_instrumentation: bool = False,
        resolvable_worker_env_vars: Collection[str] | AllowAllWorkerEnvVars = (),
    ) -> None:
        """Initialize the AgentEnvironment.

        Args:
            model_params: Configuration parameters for Temporal activity execution
                of model calls. If None, default parameters will be used.
            model_provider: Optional model provider for custom model implementations.
                Only one of model_provider or model should be provided.
                If both are provided, model_provider will be used.
            model: Optional model for custom model implementations.
                Use TestModel for mocking model responses.
                Equivalent to model_provider=TestModelProvider(model).
                Only one of model_provider or model should be provided.
                If both are provided, model_provider will be used.
            mcp_server_providers: Sequence of MCP servers to automatically register with the worker.
            register_activities: Whether to register activities during worker execution.
            add_temporal_spans: Whether to add temporal spans to traces
            use_otel_instrumentation: If set to true, enable open telemetry instrumentation.
                Warning: use_otel_instrumentation is experimental and behavior may change in future versions.
                Use with caution in production environments.
            resolvable_worker_env_vars: Names of the environment variables that
                ``temporal_worker_env_ref()`` may read on this environment's workers;
                pass ``AllowAllWorkerEnvVars()`` in place of the names to allow every
                variable.
                Warning: resolvable_worker_env_vars is experimental and behavior may change in future versions.
                Use with caution in production environments.
        """
        self._model_params = model_params
        self._model_provider = None
        if model_provider is not None:
            self._model_provider = model_provider
        elif model is not None:
            self._model_provider = TestModelProvider(model)
        self._mcp_server_providers = mcp_server_providers
        self._register_activities = register_activities
        self._plugin: OpenAIAgentsPlugin | None = None
        self._add_temporal_spans = add_temporal_spans
        self._use_otel_instrumentation = use_otel_instrumentation
        self._resolvable_worker_env_vars = _snapshot_resolvable_env_vars(
            resolvable_worker_env_vars
        )

    async def __aenter__(self) -> "AgentEnvironment":
        """Enter the async context manager."""
        # Create the plugin with the provided configuration
        self._plugin = OpenAIAgentsPlugin(
            model_params=self._model_params,
            model_provider=self._model_provider,
            mcp_server_providers=self._mcp_server_providers,
            register_activities=self._register_activities,
            add_temporal_spans=self._add_temporal_spans,
            use_otel_instrumentation=self._use_otel_instrumentation,
            resolvable_worker_env_vars=self._resolvable_worker_env_vars,
        )

        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit the async context manager."""
        # No cleanup needed currently
        pass

    def applied_on_client(self, client: Client) -> Client:
        """Apply the agent environment's plugin to a client and return a new client instance.

        Args:
            client: The base Temporal client to apply the plugin to.

        Returns:
            A new Client instance with the OpenAI agents plugin applied.
        """
        if self._plugin is None:
            raise RuntimeError(
                "AgentEnvironment must be entered before applying to client"
            )

        new_config = client.config()
        existing_plugins = new_config.get("plugins", [])
        new_config["plugins"] = list(existing_plugins) + [self._plugin]
        return Client(**new_config)

    @property
    def openai_agents_plugin(self) -> OpenAIAgentsPlugin:
        """Get the underlying OpenAI agents plugin."""
        if self._plugin is None:
            raise RuntimeError(
                "AgentEnvironment must be entered before accessing plugin"
            )
        return self._plugin
