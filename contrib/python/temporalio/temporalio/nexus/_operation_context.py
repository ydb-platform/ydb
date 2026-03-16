from __future__ import annotations

import dataclasses
import logging
from collections.abc import Awaitable, Mapping, MutableMapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
    overload,
)

from nexusrpc.handler import CancelOperationContext, StartOperationContext
from typing_extensions import Concatenate

import temporalio.api.common.v1
import temporalio.api.workflowservice.v1
import temporalio.common
from temporalio.nexus import _link_conversion
from temporalio.nexus._token import WorkflowHandle
from temporalio.types import (
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)

if TYPE_CHECKING:
    import temporalio.client

# The Temporal Nexus worker always builds a nexusrpc StartOperationContext or
# CancelOperationContext and passes it as the first parameter to the nexusrpc operation
# handler. In addition, it sets one of the following context vars.

_temporal_start_operation_context: ContextVar[_TemporalStartOperationContext] = (
    ContextVar("temporal-start-operation-context")
)

_temporal_cancel_operation_context: ContextVar[_TemporalCancelOperationContext] = (
    ContextVar("temporal-cancel-operation-context")
)


@dataclass(frozen=True)
class Info:
    """Information about the running Nexus operation.

    .. warning::
        This API is experimental and unstable.

    Retrieved inside a Nexus operation handler via :py:func:`info`.
    """

    task_queue: str
    """The task queue of the worker handling this Nexus operation."""


def in_operation() -> bool:
    """Whether the current code is inside a Nexus operation."""
    return _try_temporal_context() is not None


def info() -> Info:
    """Get the current Nexus operation information."""
    return _temporal_context().info()


def client() -> temporalio.client.Client:
    """Get the Temporal client used by the worker handling the current Nexus operation."""
    return _temporal_context().client


def _temporal_context() -> (
    Union[_TemporalStartOperationContext, _TemporalCancelOperationContext]
):
    ctx = _try_temporal_context()
    if ctx is None:
        raise RuntimeError("Not in Nexus operation context.")
    return ctx


def _try_temporal_context() -> (
    Optional[Union[_TemporalStartOperationContext, _TemporalCancelOperationContext]]
):
    start_ctx = _temporal_start_operation_context.get(None)
    cancel_ctx = _temporal_cancel_operation_context.get(None)
    if start_ctx and cancel_ctx:
        raise RuntimeError("Cannot be in both start and cancel operation contexts.")
    return start_ctx or cancel_ctx


@dataclass
class _TemporalStartOperationContext:
    """Context for a Nexus start operation being handled by a Temporal Nexus Worker."""

    nexus_context: StartOperationContext
    """Nexus-specific start operation context."""

    info: Callable[[], Info]
    """Temporal information about the running Nexus operation."""

    client: temporalio.client.Client
    """The Temporal client in use by the worker handling this Nexus operation."""

    @classmethod
    def get(cls) -> _TemporalStartOperationContext:
        ctx = _temporal_start_operation_context.get(None)
        if ctx is None:
            raise RuntimeError("Not in Nexus operation context.")
        return ctx

    def set(self) -> None:
        _temporal_start_operation_context.set(self)

    def _get_callbacks(
        self,
    ) -> list[temporalio.client.Callback]:
        ctx = self.nexus_context
        return (
            [
                NexusCallback(
                    url=ctx.callback_url,
                    headers=ctx.callback_headers,
                )
            ]
            if ctx.callback_url
            else []
        )

    def _get_workflow_event_links(
        self,
    ) -> list[temporalio.api.common.v1.Link.WorkflowEvent]:
        event_links = []
        for inbound_link in self.nexus_context.inbound_links:
            if link := _link_conversion.nexus_link_to_workflow_event(inbound_link):
                event_links.append(link)
        return event_links

    def _add_outbound_links(
        self, workflow_handle: temporalio.client.WorkflowHandle[Any, Any]
    ):
        # If links were not sent in StartWorkflowExecutionResponse then construct them.
        wf_event_links: list[temporalio.api.common.v1.Link.WorkflowEvent] = []
        try:
            if isinstance(
                workflow_handle._start_workflow_response,
                temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse,
            ):
                if workflow_handle._start_workflow_response.HasField("link"):
                    if link := workflow_handle._start_workflow_response.link:
                        if link.HasField("workflow_event"):
                            wf_event_links.append(link.workflow_event)
            if not wf_event_links:
                wf_event_links = [
                    _link_conversion.workflow_execution_started_event_link_from_workflow_handle(
                        workflow_handle
                    )
                ]
            self.nexus_context.outbound_links.extend(
                _link_conversion.workflow_event_to_nexus_link(link)
                for link in wf_event_links
            )
        except Exception as e:
            logger.warning(
                f"Failed to create WorkflowExecutionStarted event links for workflow {workflow_handle}: {e}"
            )
        return workflow_handle


class WorkflowRunOperationContext(StartOperationContext):
    """Context received by a workflow run operation.

    .. warning::
        This API is experimental and unstable.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the workflow run operation context."""
        super().__init__(*args, **kwargs)
        self._temporal_context = _TemporalStartOperationContext.get()

    @classmethod
    def _from_start_operation_context(
        cls, ctx: StartOperationContext
    ) -> WorkflowRunOperationContext:
        return cls(
            **{f.name: getattr(ctx, f.name) for f in dataclasses.fields(ctx)},
        )

    # Overload for no-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: Optional[str] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[ReturnType]: ...

    # Overload for single-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: Optional[str] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[ReturnType]: ...

    # Overload for multi-param workflow
    @overload
    async def start_workflow(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: Optional[str] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[ReturnType]: ...

    # Overload for string-name workflow
    @overload
    async def start_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: Optional[str] = None,
        result_type: Optional[type[ReturnType]] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[ReturnType]: ...

    async def start_workflow(
        self,
        workflow: Union[str, Callable[..., Awaitable[ReturnType]]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: Optional[str] = None,
        result_type: Optional[type] = None,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[
            Union[
                temporalio.common.TypedSearchAttributes,
                temporalio.common.SearchAttributes,
            ]
        ] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Optional[timedelta] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: Optional[temporalio.common.VersioningOverride] = None,
    ) -> WorkflowHandle[ReturnType]:
        """Start a workflow that will deliver the result of the Nexus operation.

        The workflow will be started in the same namespace as the Nexus worker, using
        the same client as the worker. If task queue is not specified, the worker's task
        queue will be used.

        See :py:meth:`temporalio.client.Client.start_workflow` for all arguments.

        The return value is :py:class:`temporalio.nexus.WorkflowHandle`.

        The workflow will be started as usual, with the following modifications:

        - On workflow completion, Temporal server will deliver the workflow result to
            the Nexus operation caller, using the callback from the Nexus operation start
            request.

        - The request ID from the Nexus operation start request will be used as the
            request ID for the start workflow request.

        - Inbound links to the caller that were submitted in the Nexus start operation
            request will be attached to the started workflow and, outbound links to the
            started workflow will be added to the Nexus start operation response. If the
            Nexus caller is itself a workflow, this means that the workflow in the caller
            namespace web UI will contain links to the started workflow, and vice versa.
        """
        # TODO(nexus-preview): When sdk-python supports on_conflict_options, Typescript does this:
        # if (workflowOptions.workflowIdConflictPolicy === 'USE_EXISTING') {
        #     internalOptions.onConflictOptions = {
        #     attachLinks: true,
        #     attachCompletionCallbacks: true,
        #     attachRequestId: true,
        #     };
        # }
        if (
            id_conflict_policy
            == temporalio.common.WorkflowIDConflictPolicy.USE_EXISTING
        ):
            raise RuntimeError(
                "WorkflowIDConflictPolicy.USE_EXISTING is not yet supported when starting a workflow "
                "that backs a Nexus operation (Python SDK Nexus support is at Pre-release stage)."
            )

        # We must pass nexus_completion_callbacks, workflow_event_links, and request_id,
        # but these are deliberately not exposed in overloads, hence the type-check
        # violation.
        wf_handle = await self._temporal_context.client.start_workflow(  # type: ignore
            workflow=workflow,
            arg=arg,
            args=args,
            id=id,
            task_queue=task_queue or self._temporal_context.info().task_queue,
            result_type=result_type,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            start_signal=start_signal,
            start_signal_args=start_signal_args,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
            request_eager_start=request_eager_start,
            priority=priority,
            versioning_override=versioning_override,
            callbacks=self._temporal_context._get_callbacks(),
            workflow_event_links=self._temporal_context._get_workflow_event_links(),
            request_id=self._temporal_context.nexus_context.request_id,
        )

        self._temporal_context._add_outbound_links(wf_handle)

        return WorkflowHandle[ReturnType]._unsafe_from_client_workflow_handle(wf_handle)


@dataclass(frozen=True)
class NexusCallback:
    """Nexus callback to attach to events such as workflow completion.

    .. warning::
        This API is experimental and unstable.
    """

    url: str
    """Callback URL."""

    headers: Mapping[str, str]
    """Header to attach to callback request."""


@dataclass(frozen=True)
class _TemporalCancelOperationContext:
    """Context for a Nexus cancel operation being handled by a Temporal Nexus Worker."""

    nexus_context: CancelOperationContext
    """Nexus-specific cancel operation context."""

    info: Callable[[], Info]
    """Temporal information about the running Nexus cancel operation."""

    client: temporalio.client.Client
    """The Temporal client in use by the worker handling the current Nexus operation."""

    @classmethod
    def get(cls) -> _TemporalCancelOperationContext:
        ctx = _temporal_cancel_operation_context.get(None)
        if ctx is None:
            raise RuntimeError("Not in Nexus cancel operation context.")
        return ctx

    def set(self) -> None:
        _temporal_cancel_operation_context.set(self)


class LoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that adds Nexus operation context information."""

    def __init__(self, logger: logging.Logger, extra: Optional[Mapping[str, Any]]):
        """Initialize the logger adapter."""
        super().__init__(logger, extra or {})

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Process log records to add Nexus operation context."""
        extra = dict(self.extra or {})
        if tctx := _try_temporal_context():
            extra["service"] = tctx.nexus_context.service
            extra["operation"] = tctx.nexus_context.operation
            extra["task_queue"] = tctx.info().task_queue
        kwargs["extra"] = extra | kwargs.get("extra", {})
        return msg, kwargs


logger = LoggerAdapter(logging.getLogger("temporalio.nexus"), None)
"""Logger that emits additional data describing the current Nexus operation."""
