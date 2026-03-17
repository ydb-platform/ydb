"""Worker interceptor."""

from __future__ import annotations

import concurrent.futures
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    Awaitable,
    Generic,
    List,
    NoReturn,
    Optional,
    Sequence,
    Type,
    Union,
)

import nexusrpc.handler
from nexusrpc import InputT, OutputT

import temporalio.activity
import temporalio.api.common.v1
import temporalio.common
import temporalio.nexus
import temporalio.nexus._util
import temporalio.workflow
from temporalio.workflow import VersioningIntent


class Interceptor:
    """Interceptor for workers.

    This should be extended by any worker interceptors.
    """

    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        """Method called for intercepting an activity.

        Args:
            next: The underlying inbound interceptor this interceptor should
                delegate to.

        Returns:
            The new interceptor that will be used to for the activity.
        """
        return next

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Optional[Type[WorkflowInboundInterceptor]]:
        """Class that will be instantiated and used to intercept workflows.

        This method is called on workflow start. The class must have the same
        init as :py:meth:`WorkflowInboundInterceptor.__init__`. The input can be
        altered to do things like add additional extern functions.

        Args:
            input: Input to this method that contains mutable properties that
                can be altered by this interceptor.

        Returns:
            The class to construct to intercept each workflow.
        """
        return None


@dataclass(frozen=True)
class WorkflowInterceptorClassInput:
    """Input for :py:meth:`Interceptor.workflow_interceptor_class`."""

    unsafe_extern_functions: MutableMapping[str, Callable]
    """Set of external functions that can be called from the sandbox.

    .. warning::
        Exposing external functions to the workflow sandbox is dangerous and
        should be avoided. Use at your own risk.

    .. warning::
        This API is experimental and subject to removal.
    """


@dataclass
class ExecuteActivityInput:
    """Input for :py:meth:`ActivityInboundInterceptor.execute_activity`."""

    fn: Callable[..., Any]
    args: Sequence[Any]
    executor: Optional[concurrent.futures.Executor]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


class ActivityInboundInterceptor:
    """Inbound interceptor to wrap outbound creation and activity execution.

    This should be extended by any activity inbound interceptors.
    """

    def __init__(self, next: ActivityInboundInterceptor) -> None:
        """Create the inbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def init(self, outbound: ActivityOutboundInterceptor) -> None:
        """Initialize with an outbound interceptor.

        To add a custom outbound interceptor, wrap the given interceptor before
        sending to the next ``init`` call.
        """
        self.next.init(outbound)

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        """Called to invoke the activity."""
        return await self.next.execute_activity(input)


class ActivityOutboundInterceptor:
    """Outbound interceptor to wrap calls made from within activities.

    This should be extended by any activity outbound interceptors.
    """

    def __init__(self, next: ActivityOutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def info(self) -> temporalio.activity.Info:
        """Called for every :py:func:`temporalio.activity.info` call."""
        return self.next.info()

    def heartbeat(self, *details: Any) -> None:
        """Called for every :py:func:`temporalio.activity.heartbeat` call."""
        self.next.heartbeat(*details)


@dataclass
class ContinueAsNewInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.continue_as_new`."""

    workflow: Optional[str]
    args: Sequence[Any]
    task_queue: Optional[str]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    versioning_intent: Optional[VersioningIntent]
    # The types may be absent
    arg_types: Optional[List[Type]]


@dataclass
class ExecuteWorkflowInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.execute_workflow`."""

    type: Type
    # Note, this is an unbound method
    run_fn: Callable[..., Awaitable[Any]]
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class HandleSignalInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_signal`."""

    signal: str
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class HandleQueryInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_query`."""

    id: str
    query: str
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class HandleUpdateInput:
    """Input for :py:meth:`WorkflowInboundInterceptor.handle_update_validator`
    and :py:meth:`WorkflowInboundInterceptor.handle_update_handler`.
    """

    id: str
    update: str
    args: Sequence[Any]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class SignalChildWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.signal_child_workflow`."""

    signal: str
    args: Sequence[Any]
    child_workflow_id: str
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class SignalExternalWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.signal_external_workflow`."""

    signal: str
    args: Sequence[Any]
    namespace: str
    workflow_id: str
    workflow_run_id: Optional[str]
    headers: Mapping[str, temporalio.api.common.v1.Payload]


@dataclass
class StartActivityInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_activity`."""

    activity: str
    args: Sequence[Any]
    activity_id: Optional[str]
    task_queue: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    heartbeat_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cancellation_type: temporalio.workflow.ActivityCancellationType
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    disable_eager_execution: bool
    versioning_intent: Optional[VersioningIntent]
    summary: Optional[str]
    priority: temporalio.common.Priority
    # The types may be absent
    arg_types: Optional[List[Type]]
    ret_type: Optional[Type]


@dataclass
class StartChildWorkflowInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_child_workflow`."""

    workflow: str
    args: Sequence[Any]
    id: str
    task_queue: Optional[str]
    cancellation_type: temporalio.workflow.ChildWorkflowCancellationType
    parent_close_policy: temporalio.workflow.ParentClosePolicy
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        Union[
            temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
        ]
    ]
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    versioning_intent: Optional[VersioningIntent]
    static_summary: Optional[str]
    static_details: Optional[str]
    priority: temporalio.common.Priority
    # The types may be absent
    arg_types: Optional[List[Type]]
    ret_type: Optional[Type]


@dataclass
class StartNexusOperationInput(Generic[InputT, OutputT]):
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_nexus_operation`."""

    endpoint: str
    service: str
    operation: Union[nexusrpc.Operation[InputT, OutputT], str, Callable[..., Any]]
    input: InputT
    schedule_to_close_timeout: Optional[timedelta]
    cancellation_type: temporalio.workflow.NexusOperationCancellationType
    headers: Optional[Mapping[str, str]]
    output_type: Optional[Type[OutputT]] = None

    def __post_init__(self) -> None:
        """Initialize operation-specific attributes after dataclass creation."""
        if isinstance(self.operation, nexusrpc.Operation):
            self.output_type = self.operation.output_type
        elif callable(self.operation):
            _, op = temporalio.nexus._util.get_operation_factory(self.operation)
            if isinstance(op, nexusrpc.Operation):
                self.output_type = op.output_type
            else:
                raise ValueError(
                    f"Operation callable is not a Nexus operation: {self.operation}"
                )
        elif isinstance(self.operation, str):
            pass
        else:
            raise ValueError(f"Operation is not a Nexus operation: {self.operation}")

    @property
    def operation_name(self) -> str:
        """Get the name of the Nexus operation."""
        if isinstance(self.operation, nexusrpc.Operation):
            return self.operation.name
        elif isinstance(self.operation, str):
            return self.operation
        elif callable(self.operation):
            _, op = temporalio.nexus._util.get_operation_factory(self.operation)
            if isinstance(op, nexusrpc.Operation):
                return op.name
            else:
                raise ValueError(
                    f"Operation callable is not a Nexus operation: {self.operation}"
                )
        else:
            raise ValueError(f"Operation is not a Nexus operation: {self.operation}")


@dataclass
class StartLocalActivityInput:
    """Input for :py:meth:`WorkflowOutboundInterceptor.start_local_activity`."""

    activity: str
    args: Sequence[Any]
    activity_id: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    local_retry_threshold: Optional[timedelta]
    cancellation_type: temporalio.workflow.ActivityCancellationType
    headers: Mapping[str, temporalio.api.common.v1.Payload]
    # The types may be absent
    arg_types: Optional[List[Type]]
    ret_type: Optional[Type]


class WorkflowInboundInterceptor:
    """Inbound interceptor to wrap outbound creation, workflow execution, and
    signal/query handling.

    This should be extended by any workflow inbound interceptors.
    """

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        """Create the inbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        """Initialize with an outbound interceptor.

        To add a custom outbound interceptor, wrap the given interceptor before
        sending to the next ``init`` call.
        """
        self.next.init(outbound)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        """Called to run the workflow."""
        return await self.next.execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        """Called to handle a signal."""
        return await self.next.handle_signal(input)

    async def handle_query(self, input: HandleQueryInput) -> Any:
        """Called to handle a query."""
        return await self.next.handle_query(input)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        """Called to handle an update's validation stage."""
        self.next.handle_update_validator(input)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        """Called to handle an update's handler."""
        return await self.next.handle_update_handler(input)


class WorkflowOutboundInterceptor:
    """Outbound interceptor to wrap calls made from within workflows.

    This should be extended by any workflow outbound interceptors.
    """

    def __init__(self, next: WorkflowOutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        """Called for every :py:func:`temporalio.workflow.continue_as_new` call."""
        self.next.continue_as_new(input)

    def info(self) -> temporalio.workflow.Info:
        """Called for every :py:func:`temporalio.workflow.info` call."""
        return self.next.info()

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        """Called for every
        :py:meth:`temporalio.workflow.ChildWorkflowHandle.signal` call.
        """
        return await self.next.signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        """Called for every
        :py:meth:`temporalio.workflow.ExternalWorkflowHandle.signal` call.
        """
        return await self.next.signal_external_workflow(input)

    def start_activity(
        self, input: StartActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        """Called for every :py:func:`temporalio.workflow.start_activity` and
        :py:func:`temporalio.workflow.execute_activity` call.
        """
        return self.next.start_activity(input)

    async def start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle[Any, Any]:
        """Called for every :py:func:`temporalio.workflow.start_child_workflow`
        and :py:func:`temporalio.workflow.execute_child_workflow` call.
        """
        return await self.next.start_child_workflow(input)

    def start_local_activity(
        self, input: StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        """Called for every :py:func:`temporalio.workflow.start_local_activity`
        and :py:func:`temporalio.workflow.execute_local_activity` call.
        """
        return self.next.start_local_activity(input)

    async def start_nexus_operation(
        self, input: StartNexusOperationInput[InputT, OutputT]
    ) -> temporalio.workflow.NexusOperationHandle[OutputT]:
        """Called for every :py:func:`temporalio.workflow.start_nexus_operation` call."""
        return await self.next.start_nexus_operation(input)
