import asyncio
import dataclasses
import functools
import inspect
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from types import TracebackType
from typing import Any, cast

from agents import AgentBase, RunContextWrapper
from agents.mcp import MCPServer
from mcp import GetPromptResult, ListPromptsResult  # type:ignore
from mcp import Tool as MCPTool  # type:ignore
from mcp.types import CallToolResult  # type:ignore

from temporalio import activity, workflow
from temporalio.api.enums.v1.workflow_pb2 import (
    TIMEOUT_TYPE_HEARTBEAT,
    TIMEOUT_TYPE_SCHEDULE_TO_START,
)
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    is_cancelled_exception,
)
from temporalio.worker import PollerBehaviorSimpleMaximum, Worker
from temporalio.workflow import ActivityConfig, ActivityHandle


@dataclasses.dataclass
class _StatelessListToolsArguments:
    factory_argument: Any | None


@dataclasses.dataclass
class _StatelessCallToolsArguments:
    tool_name: str
    arguments: dict[str, Any] | None
    factory_argument: Any | None
    meta: dict[str, Any] | None = None


@dataclasses.dataclass
class _StatelessListPromptsArguments:
    factory_argument: Any | None


@dataclasses.dataclass
class _StatelessGetPromptArguments:
    name: str
    arguments: dict[str, Any] | None
    factory_argument: Any | None


class _StatelessMCPServerReference(MCPServer):  # type:ignore[reportUnusedClass]
    def __init__(
        self,
        server: str,
        config: ActivityConfig | None,
        cache_tools_list: bool,
        factory_argument: Any | None = None,
    ):
        self._name = server + "-stateless"
        self._config = config or ActivityConfig(
            start_to_close_timeout=timedelta(minutes=1)
        )
        self._cache_tools_list = cache_tools_list
        self._tools = None
        self._factory_argument = factory_argument
        super().__init__()

    @property
    def name(self) -> str:
        return self._name

    async def connect(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def list_tools(
        self,
        run_context: RunContextWrapper[Any] | None = None,
        agent: AgentBase | None = None,
    ) -> list[MCPTool]:
        if self._tools:
            return self._tools
        tools = await workflow.execute_activity(
            self.name + "-list-tools",
            _StatelessListToolsArguments(self._factory_argument),
            result_type=list[MCPTool],
            **self._config,
        )
        if self._cache_tools_list:
            self._tools = tools
        return tools

    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        return await workflow.execute_activity(
            self.name + "-call-tool-v2",
            _StatelessCallToolsArguments(
                tool_name, arguments, self._factory_argument, meta
            ),
            result_type=CallToolResult,
            **self._config,
        )

    async def list_prompts(self) -> ListPromptsResult:
        return await workflow.execute_activity(
            self.name + "-list-prompts",
            _StatelessListPromptsArguments(self._factory_argument),
            result_type=ListPromptsResult,
            **self._config,
        )

    async def get_prompt(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> GetPromptResult:
        return await workflow.execute_activity(
            self.name + "-get-prompt-v2",
            _StatelessGetPromptArguments(name, arguments, self._factory_argument),
            result_type=GetPromptResult,
            **self._config,
        )


class StatelessMCPServerProvider:
    """A stateless MCP server implementation for Temporal workflows.

    This class wraps a function to create MCP servers to make them stateless by executing each MCP operation
    as a separate Temporal activity. Each operation (list_tools, call_tool, etc.) will
    connect to the underlying server, execute the operation, and then clean up the connection.

    This approach will not maintain state across calls. If the desired MCPServer needs persistent state in order to
    function, this cannot be used.
    """

    def __init__(
        self,
        name: str,
        server_factory: (Callable[[], MCPServer] | Callable[[Any | None], MCPServer]),
    ):
        """Initialize the stateless temporal MCP server.

        Args:
            name: The name of the MCP server.
            server_factory: A function which will produce MCPServer instances. It should return a new server each time
                so that state is not shared between workflow runs. It may accept a single positional parameter, which
                receives a ``factory_argument`` from the workflow.
        """
        self._server_factory = server_factory

        # Cache whether the server factory needs to be provided with arguments
        sig = inspect.signature(self._server_factory)
        self._server_accepts_arguments = len(sig.parameters) != 0

        self._name = name + "-stateless"
        super().__init__()

    def _create_server(self, factory_argument: Any | None) -> MCPServer:
        if self._server_accepts_arguments:
            return cast(Callable[[Any | None], MCPServer], self._server_factory)(
                factory_argument
            )
        else:
            return cast(Callable[[], MCPServer], self._server_factory)()

    @property
    def name(self) -> str:
        """Get the server name."""
        return self._name

    def _get_activities(self) -> Sequence[Callable]:
        @activity.defn(name=self.name + "-list-tools")
        async def list_tools(
            args: _StatelessListToolsArguments | None = None,
        ) -> list[MCPTool]:
            server = self._create_server(args.factory_argument if args else None)
            try:
                await server.connect()
                return await server.list_tools()
            finally:
                await server.cleanup()

        @activity.defn(name=self.name + "-call-tool-v2")
        async def call_tool(args: _StatelessCallToolsArguments) -> CallToolResult:
            server = self._create_server(args.factory_argument)
            try:
                await server.connect()
                return await server.call_tool(args.tool_name, args.arguments, args.meta)
            finally:
                await server.cleanup()

        @activity.defn(name=self.name + "-list-prompts")
        async def list_prompts(
            args: _StatelessListPromptsArguments | None = None,
        ) -> ListPromptsResult:
            server = self._create_server(args.factory_argument if args else None)
            try:
                await server.connect()
                return await server.list_prompts()
            finally:
                await server.cleanup()

        @activity.defn(name=self.name + "-get-prompt-v2")
        async def get_prompt(args: _StatelessGetPromptArguments) -> GetPromptResult:
            server = self._create_server(args.factory_argument)
            try:
                await server.connect()
                return await server.get_prompt(args.name, args.arguments)
            finally:
                await server.cleanup()

        @activity.defn(name=self.name + "-call-tool")
        async def call_tool_deprecated(
            tool_name: str,
            arguments: dict[str, Any] | None,
        ) -> CallToolResult:
            return await call_tool(
                _StatelessCallToolsArguments(tool_name, arguments, None)
            )

        @activity.defn(name=self.name + "-get-prompt")
        async def get_prompt_deprecated(
            name: str,
            arguments: dict[str, Any] | None,
        ) -> GetPromptResult:
            return await get_prompt(_StatelessGetPromptArguments(name, arguments, None))

        return (
            list_tools,
            call_tool,
            list_prompts,
            get_prompt,
            call_tool_deprecated,
            get_prompt_deprecated,
        )


def _handle_worker_failure(func: Callable) -> Callable:
    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any):
        try:
            return await func(*args, **kwargs)
        except ActivityError as e:
            failure = e.failure
            if failure:
                cause = failure.cause
                if cause:
                    if (
                        cause.timeout_failure_info.timeout_type
                        == TIMEOUT_TYPE_SCHEDULE_TO_START
                    ):
                        raise ApplicationError(
                            "MCP Stateful Server Worker failed to schedule activity.",
                            type="DedicatedWorkerFailure",
                        ) from e
                    if (
                        cause.timeout_failure_info.timeout_type
                        == TIMEOUT_TYPE_HEARTBEAT
                    ):
                        raise ApplicationError(
                            "MCP Stateful Server Worker failed to heartbeat.",
                            type="DedicatedWorkerFailure",
                        ) from e
            raise e

    return wrapper


@dataclasses.dataclass
class _StatefulCallToolsArguments:
    tool_name: str
    arguments: dict[str, Any] | None
    meta: dict[str, Any] | None = None


@dataclasses.dataclass
class _StatefulGetPromptArguments:
    name: str
    arguments: dict[str, Any] | None


@dataclasses.dataclass
class _StatefulServerSessionArguments:
    factory_argument: Any | None


class _StatefulMCPServerReference(MCPServer, AbstractAsyncContextManager):  # type:ignore[reportUnusedClass]
    def __init__(
        self,
        server: str,
        config: ActivityConfig | None,
        server_session_config: ActivityConfig | None,
        factory_argument: Any | None,
    ):
        self._name = server + "-stateful"
        self._config = config or ActivityConfig(
            start_to_close_timeout=timedelta(minutes=1),
            schedule_to_start_timeout=timedelta(seconds=30),
        )
        self._server_session_config = server_session_config or ActivityConfig(
            start_to_close_timeout=timedelta(hours=1),
        )
        self._connect_handle: ActivityHandle | None = None
        self._factory_argument = factory_argument
        super().__init__()

    @property
    def name(self) -> str:
        return self._name

    async def connect(self) -> None:
        self._config["task_queue"] = self.name + "@" + workflow.info().run_id
        self._connect_handle = workflow.start_activity(
            self.name + "-server-session",
            _StatefulServerSessionArguments(self._factory_argument),
            **self._server_session_config,
        )

    async def cleanup(self) -> None:
        if self._connect_handle:
            self._connect_handle.cancel()
            try:
                await self._connect_handle
            except Exception as e:
                if is_cancelled_exception(e):
                    pass
                else:
                    raise

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.cleanup()

    @_handle_worker_failure
    async def list_tools(
        self,
        run_context: RunContextWrapper[Any] | None = None,
        agent: AgentBase | None = None,
    ) -> list[MCPTool]:
        if not self._connect_handle:
            raise ApplicationError(
                "Stateful MCP Server not connected. Call connect first."
            )
        return await workflow.execute_activity(
            self.name + "-list-tools",
            args=[],
            result_type=list[MCPTool],
            **self._config,
        )

    @_handle_worker_failure
    async def call_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any] | None,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        if not self._connect_handle:
            raise ApplicationError(
                "Stateful MCP Server not connected. Call connect first."
            )
        return await workflow.execute_activity(
            self.name + "-call-tool-v2",
            _StatefulCallToolsArguments(tool_name, arguments, meta),
            result_type=CallToolResult,
            **self._config,
        )

    @_handle_worker_failure
    async def list_prompts(self) -> ListPromptsResult:
        if not self._connect_handle:
            raise ApplicationError(
                "Stateful MCP Server not connected. Call connect first."
            )
        return await workflow.execute_activity(
            self.name + "-list-prompts",
            args=[],
            result_type=ListPromptsResult,
            **self._config,
        )

    @_handle_worker_failure
    async def get_prompt(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> GetPromptResult:
        if not self._connect_handle:
            raise ApplicationError(
                "Stateful MCP Server not connected. Call connect first."
            )
        return await workflow.execute_activity(
            self.name + "-get-prompt-v2",
            _StatefulGetPromptArguments(name, arguments),
            result_type=GetPromptResult,
            **self._config,
        )


class StatefulMCPServerProvider:
    """A stateful MCP server implementation for Temporal workflows.

    This class wraps a function to create MCP servers to maintain a persistent connection throughout
    the workflow execution. It creates a dedicated worker that stays connected to
    the MCP server and processes operations on a dedicated task queue.

    This approach will allow the MCPServer to maintain state across calls if needed, but the caller
    will have to handle cases where the dedicated worker fails, as Temporal is unable to seamlessly
    recreate any lost state in that case. It is discouraged to use this approach unless necessary.

    Handling dedicated worker failure will entail catching ApplicationError with type "DedicatedWorkerFailure".
    Depending on the usage pattern, the caller will then have to either restart from the point at which the Stateful
    server was needed or handle continuing from that loss of state in some other way.
    """

    def __init__(
        self,
        name: str,
        server_factory: Callable[[Any | None], MCPServer],
    ):
        """Initialize the stateful temporal MCP server.

        Args:
            name: The name of the MCP server.
            server_factory: A function which will produce MCPServer instances. It should return a new server each time
                so that state is not shared between workflow runs. It receives an optional ``factory_argument`` from the
                workflow.
        """
        self._server_factory = server_factory
        self._name = name + "-stateful"
        self._connect_handle: ActivityHandle | None = None
        self._servers: dict[str, MCPServer] = {}
        super().__init__()

    @property
    def name(self) -> str:
        """Get the server name."""
        return self._name

    def _get_activities(self) -> Sequence[Callable]:
        def _server_id():
            return self.name + "@" + (activity.info().workflow_run_id or "")

        @activity.defn(name=self.name + "-list-tools")
        async def list_tools() -> list[MCPTool]:
            return await self._servers[_server_id()].list_tools()

        @activity.defn(name=self.name + "-call-tool")
        async def call_tool_deprecated(
            tool_name: str, arguments: dict[str, Any] | None
        ) -> CallToolResult:
            return await self._servers[_server_id()].call_tool(tool_name, arguments)

        @activity.defn(name=self.name + "-call-tool-v2")
        async def call_tool(args: _StatefulCallToolsArguments) -> CallToolResult:
            return await self._servers[_server_id()].call_tool(
                args.tool_name, args.arguments, args.meta
            )

        @activity.defn(name=self.name + "-list-prompts")
        async def list_prompts() -> ListPromptsResult:
            return await self._servers[_server_id()].list_prompts()

        @activity.defn(name=self.name + "-get-prompt")
        async def get_prompt_deprecated(
            name: str, arguments: dict[str, Any] | None
        ) -> GetPromptResult:
            return await self._servers[_server_id()].get_prompt(name, arguments)

        @activity.defn(name=self.name + "-get-prompt-v2")
        async def get_prompt(args: _StatefulGetPromptArguments) -> GetPromptResult:
            return await self._servers[_server_id()].get_prompt(
                args.name, args.arguments
            )

        async def heartbeat_every(delay: float, *details: Any) -> None:
            """Heartbeat every so often while not cancelled"""
            while True:
                await asyncio.sleep(delay)
                activity.heartbeat(*details)

        @activity.defn(name=self.name + "-server-session")
        async def connect(
            args: _StatefulServerSessionArguments | None = None,
        ) -> None:
            heartbeat_task = asyncio.create_task(heartbeat_every(30))

            server_id = self.name + "@" + (activity.info().workflow_run_id or "")
            if server_id in self._servers:
                raise ApplicationError(
                    "Cannot connect to an already running server. Use a distinct name if running multiple servers in one workflow."
                )
            server = self._server_factory(args.factory_argument if args else None)
            try:
                self._servers[server_id] = server
                try:
                    await server.connect()

                    worker = Worker(
                        activity.client(),
                        task_queue=server_id,
                        activities=[
                            list_tools,
                            call_tool,
                            list_prompts,
                            get_prompt,
                            call_tool_deprecated,
                            get_prompt_deprecated,
                        ],
                        activity_task_poller_behavior=PollerBehaviorSimpleMaximum(1),
                    )

                    await worker.run()
                finally:
                    await server.cleanup()
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
            finally:
                del self._servers[server_id]

        return (connect,)
