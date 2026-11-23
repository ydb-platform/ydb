from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Generator, Mapping
from datetime import timedelta
from enum import IntEnum
from typing import Any, Generic, overload

import nexusrpc
import nexusrpc.handler
from nexusrpc import InputT, OutputT

import temporalio.bridge.proto.nexus
import temporalio.nexus
from temporalio.types import NexusServiceType

from ._context import _Runtime

__all__ = [
    "NexusClient",
    "NexusOperationCancellationType",
    "NexusOperationHandle",
    "create_nexus_client",
]


class NexusOperationHandle(Generic[OutputT]):
    """Handle for interacting with a Nexus operation."""

    # TODO(nexus-preview): should attempts to instantiate directly throw?

    def cancel(self) -> bool:
        """Request cancellation of the operation."""
        raise NotImplementedError

    def __await__(self) -> Generator[Any, Any, OutputT]:
        """Support await."""
        raise NotImplementedError

    @property
    def operation_token(self) -> str | None:
        """The operation token for this handle."""
        raise NotImplementedError


class NexusOperationCancellationType(IntEnum):
    """Defines behavior of a Nexus operation when the caller workflow initiates cancellation.

    Pass one of these values to :py:meth:`NexusClient.start_operation` to define cancellation
    behavior.

    To initiate cancellation, use :py:meth:`NexusOperationHandle.cancel` and then ``await`` the
    operation handle. This will result in a :py:class:`exceptions.NexusOperationError`. The values
    of this enum define what is guaranteed to have happened by that point.
    """

    ABANDON = int(temporalio.bridge.proto.nexus.NexusOperationCancellationType.ABANDON)
    """Do not send any cancellation request to the operation handler; just report cancellation to the caller"""

    TRY_CANCEL = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.TRY_CANCEL
    )
    """Send a cancellation request but immediately report cancellation to the caller. Note that this
    does not guarantee that cancellation is delivered to the operation handler if the caller exits
    before the delivery is done.
    """

    WAIT_REQUESTED = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.WAIT_CANCELLATION_REQUESTED
    )
    """Send a cancellation request and wait for confirmation that the request was received.
    Does not wait for the operation to complete.
    """

    WAIT_COMPLETED = int(
        temporalio.bridge.proto.nexus.NexusOperationCancellationType.WAIT_CANCELLATION_COMPLETED
    )
    """Send a cancellation request and wait for the operation to complete.
    Note that the operation may not complete as cancelled (for example, if it catches the
    :py:exc:`asyncio.CancelledError` resulting from the cancellation request)."""


class NexusClient(ABC, Generic[NexusServiceType]):
    """A client for invoking Nexus operations.

    Example::

        nexus_client = workflow.create_nexus_client(
            endpoint=my_nexus_endpoint,
            service=MyService,
        )
        handle = await nexus_client.start_operation(
            operation=MyService.my_operation,
            input=MyOperationInput(value="hello"),
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        result = await handle.result()
    """

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: str,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for sync_operation methods (def)
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for operation_handler
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [NexusServiceType], nexusrpc.handler.OperationHandler[InputT, OutputT]
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    # Overload for temporal_operation methods
    @overload
    @abstractmethod
    async def start_operation(
        self,
        operation: Callable[
            [
                NexusServiceType,
                temporalio.nexus.TemporalStartOperationContext,
                temporalio.nexus.TemporalNexusClient,
                InputT,
            ],
            Awaitable[temporalio.nexus.TemporalOperationResult[OutputT]],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> NexusOperationHandle[OutputT]: ...

    @abstractmethod
    async def start_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> Any:
        """Start a Nexus operation and return its handle.

        Args:
            operation: The Nexus operation.
            input: The Nexus operation input.
            output_type: The Nexus operation output type.
            schedule_to_close_timeout: Timeout for the entire operation attempt.
            schedule_to_start_timeout: Timeout for the operation to be started.
            start_to_close_timeout: Timeout for async operations to complete after starting.
            headers: Headers to send with the Nexus HTTP request.

        Returns:
            A handle to the Nexus operation. The result can be obtained as
            ```python
            await handle.result()
            ```
        """
        ...

    # Overload for nexusrpc.Operation
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: nexusrpc.Operation[InputT, OutputT],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for string operation name
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: str,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for workflow_run_operation methods
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, temporalio.nexus.WorkflowRunOperationContext, InputT],
            Awaitable[temporalio.nexus.WorkflowHandle[OutputT]],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for sync_operation methods (async def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            Awaitable[OutputT],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for sync_operation methods (def)
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType, nexusrpc.handler.StartOperationContext, InputT],
            OutputT,
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for operation_handler
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [NexusServiceType],
            nexusrpc.handler.OperationHandler[InputT, OutputT],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    # Overload for temporal_operation methods
    @overload
    @abstractmethod
    async def execute_operation(
        self,
        operation: Callable[
            [
                NexusServiceType,
                temporalio.nexus.TemporalStartOperationContext,
                temporalio.nexus.TemporalNexusClient,
                InputT,
            ],
            Awaitable[temporalio.nexus.TemporalOperationResult[OutputT]],
        ],
        input: InputT,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> OutputT: ...

    @abstractmethod
    async def execute_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> Any:
        """Execute a Nexus operation and return its result.

        Args:
            operation: The Nexus operation.
            input: The Nexus operation input.
            output_type: The Nexus operation output type.
            schedule_to_close_timeout: Timeout for the entire operation attempt.
            schedule_to_start_timeout: Timeout for the operation to be started.
            start_to_close_timeout: Timeout for async operations to complete after starting.
            headers: Headers to send with the Nexus HTTP request.

        Returns:
            The operation result.
        """
        ...


class _NexusClient(NexusClient[NexusServiceType]):
    def __init__(
        self,
        *,
        endpoint: str,
        service: type[NexusServiceType] | str,
    ) -> None:
        """Create a Nexus client.

        Args:
            service: The Nexus service.
            endpoint: The Nexus endpoint.
        """
        # If service is not a str, then it must be a service interface or implementation
        # class.
        if isinstance(service, str):
            self.service_name = service
        elif service_defn := nexusrpc.get_service_definition(service):
            self.service_name = service_defn.name
        else:
            raise ValueError(
                f"`service` may be a name (str), or a class decorated with either "
                f"@nexusrpc.handler.service_handler or @nexusrpc.service. "
                f"Invalid service type: {type(service)}"
            )
        self.endpoint = endpoint

    async def start_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> Any:
        return await _Runtime.current().workflow_start_nexus_operation(
            endpoint=self.endpoint,
            service=self.service_name,
            operation=operation,
            input=input,
            output_type=output_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            cancellation_type=cancellation_type,
            headers=headers,
            summary=summary,
        )

    async def execute_operation(
        self,
        operation: Any,
        input: Any,
        *,
        output_type: type[OutputT] | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        cancellation_type: NexusOperationCancellationType = NexusOperationCancellationType.WAIT_COMPLETED,
        headers: Mapping[str, str] | None = None,
        summary: str | None = None,
    ) -> Any:
        handle = await self.start_operation(
            operation,
            input,
            output_type=output_type,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            cancellation_type=cancellation_type,
            headers=headers,
            summary=summary,
        )
        return await handle


@overload
def create_nexus_client(
    *,
    service: type[NexusServiceType],
    endpoint: str,
) -> NexusClient[NexusServiceType]: ...


@overload
def create_nexus_client(
    *,
    service: str,
    endpoint: str,
) -> NexusClient[Any]: ...


def create_nexus_client(
    *,
    service: type[NexusServiceType] | str,
    endpoint: str,
) -> NexusClient[NexusServiceType]:
    """Create a Nexus client.

    Args:
        service: The Nexus service.
        endpoint: The Nexus endpoint.
    """
    return _NexusClient(endpoint=endpoint, service=service)
