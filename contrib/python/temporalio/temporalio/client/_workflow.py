"""Client support for accessing Temporal."""

from __future__ import annotations

import asyncio
import functools
import warnings
from asyncio import Future
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
)
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    cast,
    overload,
)

import google.protobuf.json_format
from typing_extensions import Self

import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.history.v1
import temporalio.api.update.v1
import temporalio.api.workflow.v1
import temporalio.api.workflowservice.v1
import temporalio.common
import temporalio.converter
import temporalio.converter._search_attributes
import temporalio.exceptions
import temporalio.workflow
from temporalio.converter import (
    WorkflowSerializationContext,
)
from temporalio.service import (
    RPCError,
    RPCStatusCode,
)

from ..types import (
    AnyType,
    LocalReturnType,
    MethodAsyncNoParam,
    MethodAsyncSingleParam,
    MethodSyncOrAsyncNoParam,
    MethodSyncOrAsyncSingleParam,
    MultiParamSpec,
    ParamType,
    ReturnType,
    SelfType,
)
from ._exceptions import (
    WorkflowContinuedAsNewError,
    WorkflowFailureError,
    WorkflowUpdateFailedError,
    WorkflowUpdateRPCTimeoutOrCancelledError,
)
from ._helpers import _decode_user_metadata, _history_from_json
from ._interceptor import (
    CancelWorkflowInput,
    DescribeWorkflowInput,
    FetchWorkflowHistoryEventsInput,
    QueryWorkflowInput,
    SignalWorkflowInput,
    StartWorkflowUpdateInput,
    TerminateWorkflowInput,
    UpdateWithStartStartWorkflowInput,
)

if TYPE_CHECKING:
    from ._client import Client
    from ._interceptor import ListWorkflowsInput


class WorkflowHistoryEventFilterType(IntEnum):
    """Type of history events to get for a workflow.

    See :py:class:`temporalio.api.enums.v1.HistoryEventFilterType`.
    """

    ALL_EVENT = int(
        temporalio.api.enums.v1.HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
    )
    CLOSE_EVENT = int(
        temporalio.api.enums.v1.HistoryEventFilterType.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
    )


class WorkflowHandle(Generic[SelfType, ReturnType]):
    """Handle for interacting with a workflow.

    This is usually created via :py:meth:`Client.get_workflow_handle` or
    returned from :py:meth:`Client.start_workflow`.
    """

    def __init__(
        self,
        client: Client,
        id: str,
        *,
        run_id: str | None = None,
        result_run_id: str | None = None,
        first_execution_run_id: str | None = None,
        result_type: type | None = None,
        start_workflow_response: None
        | (
            temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse
            | temporalio.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse
        ) = None,
    ) -> None:
        """Create workflow handle."""
        self._client = client
        self._id = id
        self._run_id = run_id
        self._result_run_id = result_run_id
        self._first_execution_run_id = first_execution_run_id
        self._result_type = result_type
        self._start_workflow_response = start_workflow_response
        self.__temporal_eagerly_started = False

    @functools.cached_property
    def _data_converter(self) -> temporalio.converter.DataConverter:
        return self._client.data_converter.with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=self._client.namespace, workflow_id=self._id
            )
        )

    @property
    def id(self) -> str:
        """ID of the workflow."""
        return self._id

    @property
    def run_id(self) -> str | None:
        """If present, run ID used to ensure that requested operations apply
        to this exact run.

        This is only created via :py:meth:`Client.get_workflow_handle`.
        :py:meth:`Client.start_workflow` will not set this value.

        This cannot be mutated. If a different run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._run_id

    @property
    def result_run_id(self) -> str | None:
        """Run ID used for :py:meth:`result` calls if present to ensure result
        is for a workflow starting from this run.

        When this handle is created via :py:meth:`Client.get_workflow_handle`,
        this is the same as run_id. When this handle is created via
        :py:meth:`Client.start_workflow`, this value will be the resulting run
        ID.

        This cannot be mutated. If a different run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._result_run_id

    @property
    def first_execution_run_id(self) -> str | None:
        """Run ID used to ensure requested operations apply to a workflow ID
        started with this run ID.

        This can be set when using :py:meth:`Client.get_workflow_handle`. When
        :py:meth:`Client.start_workflow` is called without a start signal, this
        is set to the resulting run.

        This cannot be mutated. If a different first execution run ID is needed,
        :py:meth:`Client.get_workflow_handle` must be used instead.
        """
        return self._first_execution_run_id

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> ReturnType:
        """Wait for result of the workflow.

        This will use :py:attr:`result_run_id` if present to base the result on.
        To use another run ID, a new handle must be created via
        :py:meth:`Client.get_workflow_handle`.

        Args:
            follow_runs: If true (default), workflow runs will be continually
                fetched, until the most recent one is found. If false, return
                the result from the first run targeted by the request if that run
                ends in a result, otherwise raise an exception.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call. Note,
                this is the timeout for each history RPC call not this overall
                function.

        Returns:
            Result of the workflow after being converted by the data converter.

        Raises:
            WorkflowFailureError: Workflow failed, was cancelled, was
                terminated, or timed out. Use the
                :py:attr:`WorkflowFailureError.cause` to see the underlying
                reason.
            Exception: Other possible failures during result fetching.
        """
        # We have to maintain our own run ID because it can change if we follow
        # executions
        hist_run_id = self._result_run_id
        while True:
            async for event in self._fetch_history_events_for_run(
                hist_run_id,
                wait_new_event=True,
                event_filter_type=WorkflowHistoryEventFilterType.CLOSE_EVENT,
                skip_archival=True,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            ):
                if event.HasField("workflow_execution_completed_event_attributes"):
                    complete_attr = event.workflow_execution_completed_event_attributes
                    # Follow execution
                    if follow_runs and complete_attr.new_execution_run_id:
                        hist_run_id = complete_attr.new_execution_run_id
                        break
                    # Ignoring anything after the first response like TypeScript
                    type_hints = [self._result_type] if self._result_type else None
                    results = await self._data_converter.decode_wrapper(
                        complete_attr.result,
                        type_hints,
                    )
                    if not results:
                        return cast(ReturnType, None)
                    elif len(results) > 1:
                        warnings.warn(f"Expected single result, got {len(results)}")
                    return cast(ReturnType, results[0])
                elif event.HasField("workflow_execution_failed_event_attributes"):
                    fail_attr = event.workflow_execution_failed_event_attributes
                    # Follow execution
                    if follow_runs and fail_attr.new_execution_run_id:
                        hist_run_id = fail_attr.new_execution_run_id
                        break
                    raise WorkflowFailureError(
                        cause=await self._data_converter.decode_failure(
                            fail_attr.failure
                        ),
                    )
                elif event.HasField("workflow_execution_canceled_event_attributes"):
                    cancel_attr = event.workflow_execution_canceled_event_attributes
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.CancelledError(
                            "Workflow cancelled",
                            *(
                                await self._data_converter.decode_wrapper(
                                    cancel_attr.details
                                )
                            ),
                        )
                    )
                elif event.HasField("workflow_execution_terminated_event_attributes"):
                    term_attr = event.workflow_execution_terminated_event_attributes
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.TerminatedError(
                            term_attr.reason or "Workflow terminated",
                            *(
                                await self._data_converter.decode_wrapper(
                                    term_attr.details
                                )
                            ),
                        ),
                    )
                elif event.HasField("workflow_execution_timed_out_event_attributes"):
                    time_attr = event.workflow_execution_timed_out_event_attributes
                    # Follow execution
                    if follow_runs and time_attr.new_execution_run_id:
                        hist_run_id = time_attr.new_execution_run_id
                        break
                    raise WorkflowFailureError(
                        cause=temporalio.exceptions.TimeoutError(
                            "Workflow timed out",
                            type=temporalio.exceptions.TimeoutType.START_TO_CLOSE,
                            last_heartbeat_details=[],
                        ),
                    )
                elif event.HasField(
                    "workflow_execution_continued_as_new_event_attributes"
                ):
                    cont_attr = (
                        event.workflow_execution_continued_as_new_event_attributes
                    )
                    if not cont_attr.new_execution_run_id:
                        raise RuntimeError(
                            "Unexpectedly missing new run ID from continue as new"
                        )
                    # Follow execution
                    if follow_runs:
                        hist_run_id = cont_attr.new_execution_run_id
                        break
                    raise WorkflowContinuedAsNewError(cont_attr.new_execution_run_id)
            # This is reached on break which means that there's a different run
            # ID if we're following. If there's not, it's an error because no
            # event was given (should never happen).
            if hist_run_id is None:
                raise RuntimeError("No completion event found")

    async def cancel(
        self,
        *,
        reason: str = "",
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Cancel the workflow.

        This will issue a cancellation for :py:attr:`run_id` if present. This
        call will make sure to use the run chain starting from
        :py:attr:`first_execution_run_id` if present. To create handles with
        these values, use :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` with
            a start signal will cancel the latest workflow with the same
            workflow ID even if it is unrelated to the started workflow.

        Args:
            reason: Reason recorded with the cancellation request. Available
                inside the workflow via :py:func:`temporalio.workflow.cancellation_reason`.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be cancelled.
        """
        await self._client._impl.cancel_workflow(
            CancelWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                reason=reason,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def describe(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowExecutionDescription:
        """Get workflow details.

        This will get details for :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            describe the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Workflow details.

        Raises:
            RPCError: Workflow details could not be fetched.
        """
        return await self._client._impl.describe_workflow(
            DescribeWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def fetch_history(
        self,
        *,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowHistory:
        """Get workflow history.

        This is a shortcut for :py:meth:`fetch_history_events` that just fetches
        all events.
        """
        return WorkflowHistory(
            workflow_id=self.id,
            events=[
                v
                async for v in self.fetch_history_events(
                    event_filter_type=event_filter_type,
                    skip_archival=skip_archival,
                    rpc_metadata=rpc_metadata,
                    rpc_timeout=rpc_timeout,
                )
            ],
        )

    def fetch_history_events(
        self,
        *,
        page_size: int | None = None,
        next_page_token: bytes | None = None,
        wait_new_event: bool = False,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowHistoryEventAsyncIterator:
        """Get workflow history events as an async iterator.

        This does not make a request until the first iteration is attempted.
        Therefore any errors will not occur until then.

        Args:
            page_size: Maximum amount to fetch per request if any maximum.
            next_page_token: A specific page token to fetch.
            wait_new_event: Whether the event fetching request will wait for new
                events or just return right away.
            event_filter_type: Which events to obtain.
            skip_archival: Whether to skip archival.
            rpc_metadata: Headers used on each RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call.

        Returns:
            An async iterator that doesn't begin fetching until iterated on.
        """
        return self._fetch_history_events_for_run(
            self._run_id,
            page_size=page_size,
            next_page_token=next_page_token,
            wait_new_event=wait_new_event,
            event_filter_type=event_filter_type,
            skip_archival=skip_archival,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    def _fetch_history_events_for_run(
        self,
        run_id: str | None,
        *,
        page_size: int | None = None,
        next_page_token: bytes | None = None,
        wait_new_event: bool = False,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowHistoryEventAsyncIterator:
        return self._client._impl.fetch_workflow_history_events(
            FetchWorkflowHistoryEventsInput(
                id=self._id,
                run_id=run_id,
                page_size=page_size,
                next_page_token=next_page_token,
                wait_new_event=wait_new_event,
                event_filter_type=event_filter_type,
                skip_archival=skip_archival,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param query
    @overload
    async def query(
        self,
        query: MethodSyncOrAsyncNoParam[SelfType, LocalReturnType],
        *,
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for single-param query
    @overload
    async def query(
        self,
        query: MethodSyncOrAsyncSingleParam[SelfType, ParamType, LocalReturnType],
        arg: ParamType,
        *,
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param query
    @overload
    async def query(
        self,
        query: Callable[
            Concatenate[SelfType, MultiParamSpec],
            Awaitable[LocalReturnType] | LocalReturnType,
        ],
        *,
        args: Sequence[Any],
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for string-name query
    @overload
    async def query(
        self,
        query: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: type | None = None,
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any: ...

    async def query(
        self,
        query: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: type | None = None,
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Query the workflow.

        This will query for :py:attr:`run_id` if present. To use a different
        run ID, create a new handle with
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            query the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            query: Query function or name on the workflow.
            arg: Single argument to the query.
            args: Multiple arguments to the query. Cannot be set if arg is.
            result_type: For string queries, this can set the specific result
                type hint to deserialize into.
            reject_condition: Condition for rejecting the query. If unset/None,
                defaults to the client's default (which is defaulted to None).
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Returns:
            Result of the query.

        Raises:
            WorkflowQueryRejectedError: A query reject condition was satisfied.
            RPCError: Workflow details could not be fetched.
        """
        query_name: str
        ret_type = result_type
        if callable(query):
            defn = temporalio.workflow._QueryDefinition.from_fn(query)
            if not defn:
                raise RuntimeError(
                    f"Query definition not found on {query.__qualname__}, "
                    "is it decorated with @workflow.query?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic query definition")
            # TODO(cretz): Check count/type of args at runtime?
            query_name = defn.name
            ret_type = defn.ret_type
        else:
            query_name = str(query)

        return await self._client._impl.query_workflow(
            QueryWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                query=query_name,
                args=temporalio.common._arg_or_args(arg, args),
                reject_condition=reject_condition
                or self._client._config["default_workflow_query_reject_condition"],
                headers={},
                ret_type=ret_type,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param signal
    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncNoParam[SelfType, None],
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    # Overload for single-param signal
    @overload
    async def signal(
        self,
        signal: MethodSyncOrAsyncSingleParam[SelfType, ParamType, None],
        arg: ParamType,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    # Overload for multi-param signal
    @overload
    async def signal(
        self,
        signal: Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[None] | None],
        *,
        args: Sequence[Any],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    # Overload for string-name signal
    @overload
    async def signal(
        self,
        signal: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None: ...

    async def signal(
        self,
        signal: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Send a signal to the workflow.

        This will signal for :py:attr:`run_id` if present. To use a different
        run ID, create a new handle with via
        :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` will
            signal the latest workflow with the same workflow ID even if it is
            unrelated to the started workflow.

        Args:
            signal: Signal function or name on the workflow.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be signalled.
        """
        await self._client._impl.signal_workflow(
            SignalWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                signal=temporalio.workflow._SignalDefinition.must_name_from_fn_or_str(
                    signal
                ),
                args=temporalio.common._arg_or_args(arg, args),
                headers={},
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    async def terminate(
        self,
        *args: Any,
        reason: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        """Terminate the workflow.

        This will issue a termination for :py:attr:`run_id` if present. This
        call will make sure to use the run chain starting from
        :py:attr:`first_execution_run_id` if present. To create handles with
        these values, use :py:meth:`Client.get_workflow_handle`.

        .. warning::
            Handles created as a result of :py:meth:`Client.start_workflow` with
            a start signal will terminate the latest workflow with the same
            workflow ID even if it is unrelated to the started workflow.

        Args:
            args: Details to store on the termination.
            reason: Reason for the termination.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            RPCError: Workflow could not be terminated.
        """
        await self._client._impl.terminate_workflow(
            TerminateWorkflowInput(
                id=self._id,
                run_id=self._run_id,
                args=args,
                reason=reason,
                first_execution_run_id=self._first_execution_run_id,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )
        )

    # Overload for no-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for single-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for multi-param update
    @overload
    async def execute_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # type: ignore
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType: ...

    # Overload for string-name update
    @overload
    async def execute_update(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any: ...

    async def execute_update(
        self,
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> Any:
        """Send an update request to the workflow and wait for it to complete.

        This will target the workflow with :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via :py:meth:`Client.get_workflow_handle`.

        Args:
            update: Update function or name on the workflow.
            arg: Single argument to the update.
            args: Multiple arguments to the update. Cannot be set if arg is.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed
                out or cancelled.
            RPCError: There was some issue sending the update to the workflow.
        """
        handle = await self._start_update(
            update,
            arg,
            args=args,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )
        return await handle.result()

    # Overload for no-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[[SelfType], LocalReturnType],
        *,
        wait_for_stage: WorkflowUpdateStage,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for single-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            [SelfType, ParamType], LocalReturnType
        ],
        arg: ParamType,
        *,
        wait_for_stage: WorkflowUpdateStage,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for multi-param start update
    @overload
    async def start_update(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[
            MultiParamSpec, LocalReturnType
        ],
        *,
        args: MultiParamSpec.args,  # type: ignore
        wait_for_stage: WorkflowUpdateStage,
        id: str | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]: ...

    # Overload for string-name start update
    @overload
    async def start_update(
        self,
        update: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[Any]: ...

    async def start_update(
        self,
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[Any]:
        """Send an update request to the workflow and return a handle to it.

        This will target the workflow with :py:attr:`run_id` if present. To use a
        different run ID, create a new handle with via :py:meth:`Client.get_workflow_handle`.

        Args:
            update: Update function or name on the workflow. arg: Single argument to the
                update.
            wait_for_stage: Required stage to wait until returning: either ACCEPTED or
                COMPLETED. ADMITTED is not currently supported. See
                https://docs.temporal.io/workflows#update for more details.
            args: Multiple arguments to the update. Cannot be set if arg is.
            id: ID of the update. If not set, the default is a new UUID.
            result_type: For string updates, this can set the specific result
                type hint to deserialize into.
            rpc_metadata: Headers used on the RPC call. Keys here override
                client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for the RPC call.

        Raises:
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed out or
                cancelled.
            RPCError: There was some issue sending the update to the workflow.
        """
        return await self._start_update(
            update,
            arg,
            wait_for_stage=wait_for_stage,
            args=args,
            id=id,
            result_type=result_type,
            rpc_metadata=rpc_metadata,
            rpc_timeout=rpc_timeout,
        )

    async def _start_update(
        self,
        update: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        wait_for_stage: WorkflowUpdateStage,
        args: Sequence[Any] = [],
        id: str | None = None,
        result_type: type | None = None,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> WorkflowUpdateHandle[Any]:
        if wait_for_stage == WorkflowUpdateStage.ADMITTED:
            raise ValueError("ADMITTED wait stage not supported")

        update_name, result_type_from_type_hint = (
            temporalio.workflow._UpdateDefinition.get_name_and_result_type(update)
        )

        return await self._client._impl.start_workflow_update(
            StartWorkflowUpdateInput(
                id=self._id,
                run_id=self._run_id,
                first_execution_run_id=self._first_execution_run_id,
                update_id=id,
                update=update_name,
                args=temporalio.common._arg_or_args(arg, args),
                headers={},
                ret_type=result_type or result_type_from_type_hint,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
                wait_for_stage=wait_for_stage,
            )
        )

    def get_update_handle(
        self,
        id: str,
        *,
        workflow_run_id: str | None = None,
        result_type: type | None = None,
    ) -> WorkflowUpdateHandle[Any]:
        """Get a handle for an update. The handle can be used to wait on the
        update result.

        Users may prefer the more typesafe :py:meth:`get_update_handle_for`
        which accepts an update definition.

        Args:
            id: Update ID to get a handle to.
            workflow_run_id: Run ID to tie the handle to. If this is not set,
                the :py:attr:`run_id` will be used.
            result_type: The result type to deserialize into if known.

        Returns:
            The update handle.
        """
        return WorkflowUpdateHandle(
            self._client,
            id,
            self._id,
            workflow_run_id=workflow_run_id or self._run_id,
            result_type=result_type,
        )

    def get_update_handle_for(
        self,
        update: temporalio.workflow.UpdateMethodMultiParam[Any, LocalReturnType],
        id: str,
        *,
        workflow_run_id: str | None = None,
    ) -> WorkflowUpdateHandle[LocalReturnType]:
        """Get a typed handle for an update. The handle can be used to wait on
        the update result.

        This is the same as :py:meth:`get_update_handle` but typed.

        Args:
            update: The update method to use for typing the handle.
            id: Update ID to get a handle to.
            workflow_run_id: Run ID to tie the handle to. If this is not set,
                the :py:attr:`run_id` will be used.

        Returns:
            The update handle.
        """
        return self.get_update_handle(
            id, workflow_run_id=workflow_run_id, result_type=update._defn.ret_type
        )


class WithStartWorkflowOperation(Generic[SelfType, ReturnType]):
    """Defines a start-workflow operation used by update-with-start requests.

    Update-With-Start allows you to send an update to a workflow, while starting the
    workflow if necessary.
    """

    # Overload for no-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: MethodAsyncNoParam[SelfType, ReturnType],
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> None: ...

    # Overload for single-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: MethodAsyncSingleParam[SelfType, ParamType, ReturnType],
        arg: ParamType,
        *,
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> None: ...

    # Overload for multi-param workflow, with_start
    @overload
    def __init__(
        self,
        workflow: Callable[
            Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]
        ],
        *,
        args: Sequence[Any],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> None: ...

    # Overload for string-name workflow, with_start
    @overload
    def __init__(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
    ) -> None: ...

    def __init__(
        self,
        workflow: str | Callable[..., Awaitable[Any]],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy,
        result_type: type | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: None
        | (
            temporalio.common.TypedSearchAttributes | temporalio.common.SearchAttributes
        ) = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        start_delay: timedelta | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
        versioning_override: temporalio.common.VersioningOverride | None = None,
        stack_level: int = 2,
    ) -> None:
        """Create a WithStartWorkflowOperation.

        See :py:meth:`temporalio.client.Client.start_workflow` for documentation of the
        arguments.
        """
        temporalio.common._warn_on_deprecated_search_attributes(
            search_attributes, stack_level=stack_level
        )
        name, result_type_from_run_fn = (
            temporalio.workflow._Definition.get_name_and_result_type(workflow)
        )
        if id_conflict_policy == temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED:
            raise ValueError("WorkflowIDConflictPolicy is required")

        self._start_workflow_input = UpdateWithStartStartWorkflowInput(
            workflow=name,
            args=temporalio.common._arg_or_args(arg, args),
            id=id,
            task_queue=task_queue,
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
            headers={},
            ret_type=result_type or result_type_from_run_fn,
            priority=priority,
            versioning_override=versioning_override,
        )
        self._workflow_handle: Future[WorkflowHandle[SelfType, ReturnType]] = Future()
        self._used = False

    async def workflow_handle(self) -> WorkflowHandle[SelfType, ReturnType]:
        """Wait until workflow is running and return a WorkflowHandle."""
        return await self._workflow_handle


@dataclass
class WorkflowExecution:
    """Info for a single workflow execution run."""

    close_time: datetime | None
    """When the workflow was closed if closed."""

    execution_time: datetime | None
    """When this workflow run started or should start."""

    history_length: int
    """Number of events in the history."""

    id: str
    """ID for the workflow."""

    namespace: str
    """Namespace for the workflow."""

    parent_id: str | None
    """ID for the parent workflow if this was started as a child."""

    parent_run_id: str | None
    """Run ID for the parent workflow if this was started as a child."""

    root_id: str | None
    """ID for the root workflow."""

    root_run_id: str | None
    """Run ID for the root workflow."""

    raw_info: temporalio.api.workflow.v1.WorkflowExecutionInfo
    """Underlying protobuf info."""

    run_id: str
    """Run ID for this workflow run."""

    search_attributes: temporalio.common.SearchAttributes
    """Current set of search attributes if any.

    .. deprecated::
        Use :py:attr:`typed_search_attributes` instead.
    """

    start_time: datetime
    """When the workflow was created."""

    status: WorkflowExecutionStatus | None
    """Status for the workflow."""

    task_queue: str
    """Task queue for the workflow."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes
    """Current set of search attributes if any."""

    workflow_type: str
    """Type name for the workflow."""

    _context_free_data_converter: temporalio.converter.DataConverter

    @property
    def data_converter(self) -> temporalio.converter.DataConverter:
        """Data converter for the workflow."""
        return self._context_free_data_converter.with_context(
            WorkflowSerializationContext(
                namespace=self.namespace,
                workflow_id=self.id,
            )
        )

    @classmethod
    def _from_raw_info(
        cls,
        info: temporalio.api.workflow.v1.WorkflowExecutionInfo,
        namespace: str,
        converter: temporalio.converter.DataConverter,
        **additional_fields: Any,
    ) -> Self:
        return cls(
            close_time=(
                info.close_time.ToDatetime().replace(tzinfo=timezone.utc)
                if info.HasField("close_time")
                else None
            ),
            execution_time=(
                info.execution_time.ToDatetime().replace(tzinfo=timezone.utc)
                if info.HasField("execution_time")
                else None
            ),
            history_length=info.history_length,
            id=info.execution.workflow_id,
            namespace=namespace,
            parent_id=(
                info.parent_execution.workflow_id
                if info.HasField("parent_execution")
                else None
            ),
            parent_run_id=(
                info.parent_execution.run_id
                if info.HasField("parent_execution")
                else None
            ),
            root_id=(
                info.root_execution.workflow_id
                if info.HasField("root_execution")
                else None
            ),
            root_run_id=(
                info.root_execution.run_id if info.HasField("root_execution") else None
            ),
            raw_info=info,
            run_id=info.execution.run_id,
            search_attributes=temporalio.converter.decode_search_attributes(
                info.search_attributes
            ),
            start_time=info.start_time.ToDatetime().replace(tzinfo=timezone.utc),
            status=WorkflowExecutionStatus(info.status) if info.status else None,
            task_queue=info.task_queue,
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                info.search_attributes
            ),
            workflow_type=info.type.name,
            _context_free_data_converter=converter,
            **additional_fields,
        )

    async def memo(self) -> Mapping[str, Any]:
        """Workflow's memo values, converted without type hints.

        Since type hints are not used, the default converted values will come
        back. For example, if the memo was originally created with a dataclass,
        the value will be a dict. To convert using proper type hints, use
        :py:meth:`memo_value`.

        Returns:
            Mapping of all memo keys and they values without type hints.
        """
        return await self.data_converter._decode_memo(self.raw_info.memo)

    @overload
    async def memo_value(
        self, key: str, default: Any = temporalio.common._arg_unset
    ) -> Any: ...

    @overload
    async def memo_value(
        self, key: str, *, type_hint: type[ParamType]
    ) -> ParamType: ...

    @overload
    async def memo_value(
        self, key: str, default: AnyType, *, type_hint: type[ParamType]
    ) -> AnyType | ParamType: ...

    async def memo_value(
        self,
        key: str,
        default: Any = temporalio.common._arg_unset,
        *,
        type_hint: type | None = None,
    ) -> Any:
        """Memo value for the given key, optional default, and optional type
        hint.

        Args:
            key: Key to get memo value for.
            default: Default to use if key is not present. If unset, a
                :py:class:`KeyError` is raised when the key does not exist.
            type_hint: type hint to use when converting.

        Returns:
            Memo value, converted with the type hint if present.

        Raises:
            KeyError: Key not present and default not set.
        """
        return await self.data_converter._decode_memo_field(
            self.raw_info.memo, key, default, type_hint
        )


@dataclass
class WorkflowExecutionDescription(WorkflowExecution):
    """Description for a single workflow execution run."""

    raw_description: temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse
    """Underlying protobuf description."""

    _static_summary: str | None = None
    _static_details: str | None = None
    _metadata_decoded: bool = False

    async def static_summary(self) -> str | None:
        """Gets the single-line fixed summary for this workflow execution that may appear in
        UI/CLI. This can be in single-line Temporal markdown format.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_summary

    async def static_details(self) -> str | None:
        """Gets the general fixed details for this workflow execution that may appear in UI/CLI.
        This can be in Temporal markdown format and can span multiple lines.
        """
        if not self._metadata_decoded:
            await self._decode_metadata()
        return self._static_details

    async def _decode_metadata(self) -> None:
        """Internal method to decode metadata lazily."""
        self._static_summary, self._static_details = await _decode_user_metadata(
            self.data_converter, self.raw_description.execution_config.user_metadata
        )
        self._metadata_decoded = True

    @staticmethod
    async def _from_raw_description(
        description: temporalio.api.workflowservice.v1.DescribeWorkflowExecutionResponse,
        namespace: str,
        converter: temporalio.converter.DataConverter,
    ) -> WorkflowExecutionDescription:
        return WorkflowExecutionDescription._from_raw_info(
            description.workflow_execution_info,
            namespace=namespace,
            converter=converter,
            raw_description=description,
        )


class WorkflowExecutionStatus(IntEnum):
    """Status of a workflow execution.

    See :py:class:`temporalio.api.enums.v1.WorkflowExecutionStatus`.
    """

    RUNNING = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
    )
    COMPLETED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
    )
    FAILED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED
    )
    CANCELED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED
    )
    TERMINATED = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED
    )
    CONTINUED_AS_NEW = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
    TIMED_OUT = int(
        temporalio.api.enums.v1.WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
    )


@dataclass
class WorkflowExecutionCount:
    """Representation of a count from a count workflows call."""

    count: int
    """Approximate number of workflows matching the original query.

    If the query had a group-by clause, this is simply the sum of all the counts
    in py:attr:`groups`.
    """

    groups: Sequence[WorkflowExecutionCountAggregationGroup]
    """Groups if the query had a group-by clause, or empty if not."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse,
    ) -> WorkflowExecutionCount:
        return WorkflowExecutionCount(
            count=raw.count,
            groups=[
                WorkflowExecutionCountAggregationGroup._from_raw(g) for g in raw.groups
            ],
        )


@dataclass
class WorkflowExecutionCountAggregationGroup:
    """Aggregation group if the workflow count query had a group-by clause."""

    count: int
    """Approximate number of workflows matching the original query for this
    group.
    """

    group_values: Sequence[temporalio.common.SearchAttributeValue]
    """Search attribute values for this group."""

    @staticmethod
    def _from_raw(
        raw: temporalio.api.workflowservice.v1.CountWorkflowExecutionsResponse.AggregationGroup,
    ) -> WorkflowExecutionCountAggregationGroup:
        return WorkflowExecutionCountAggregationGroup(
            count=raw.count,
            group_values=[
                temporalio.converter._search_attributes._decode_search_attribute_value(
                    v
                )
                for v in raw.group_values
            ],
        )


class WorkflowExecutionAsyncIterator:
    """Asynchronous iterator for :py:class:`WorkflowExecution` values.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within. To consume the workflows as histories, call
    :py:meth:`map_histories`.
    """

    def __init__(
        self,
        client: Client,
        input: ListWorkflowsInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`Client.list_workflows`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: Sequence[WorkflowExecution] | None = None
        self._current_page_index = 0
        self._limit = input.limit
        self._yielded = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(self) -> Sequence[WorkflowExecution] | None:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> bytes | None:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        page_size = page_size or self._input.page_size
        if self._limit is not None and self._limit - self._yielded < page_size:
            page_size = self._limit - self._yielded

        resp = await self._client.workflow_service.list_workflow_executions(
            temporalio.api.workflowservice.v1.ListWorkflowExecutionsRequest(
                namespace=self._client.namespace,
                page_size=page_size,
                next_page_token=self._next_page_token or b"",
                query=self._input.query or "",
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )

        self._current_page = [
            WorkflowExecution._from_raw_info(
                v, self._client.namespace, self._client.data_converter
            )
            for v in resp.executions
        ]
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> WorkflowExecutionAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> WorkflowExecution:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        if self._limit is not None and self._yielded >= self._limit:
            raise StopAsyncIteration
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Get current, increment page index, and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            self._yielded += 1
            return ret

    async def map_histories(
        self,
        *,
        event_filter_type: WorkflowHistoryEventFilterType = WorkflowHistoryEventFilterType.ALL_EVENT,
        skip_archival: bool = False,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> AsyncIterator[WorkflowHistory]:
        """Create an async iterator consuming all workflows and calling
        :py:meth:`WorkflowHandle.fetch_history` on each one.

        This is just a shortcut for ``fetch_history``, see that method for
        parameter details.
        """
        async for v in self:
            yield await self._client.get_workflow_handle(
                v.id, run_id=v.run_id
            ).fetch_history(
                event_filter_type=event_filter_type,
                skip_archival=skip_archival,
                rpc_metadata=rpc_metadata,
                rpc_timeout=rpc_timeout,
            )


@dataclass(frozen=True)
class WorkflowHistory:
    """A workflow's ID and immutable history."""

    workflow_id: str
    """ID of the workflow."""

    events: Sequence[temporalio.api.history.v1.HistoryEvent]
    """History events for the workflow."""

    @property
    def run_id(self) -> str:
        """Run ID extracted from the first event."""
        if not self.events:
            raise RuntimeError("No events")
        if not self.events[0].HasField("workflow_execution_started_event_attributes"):
            raise RuntimeError("First event is not workflow start")
        return self.events[
            0
        ].workflow_execution_started_event_attributes.original_execution_run_id

    @staticmethod
    def from_json(workflow_id: str, history: str | dict[str, Any]) -> WorkflowHistory:
        """Construct a WorkflowHistory from an ID and a json dump of history.

        This is built to work both with Temporal UI/CLI JSON as well as
        :py:meth:`to_json` even though they are slightly different.

        Args:
            workflow_id: The workflow's ID
            history: A string or parsed-to-dict representation of workflow
                history

        Returns:
            Workflow history
        """
        parsed = _history_from_json(history)
        return WorkflowHistory(workflow_id, parsed.events)

    def to_json(self) -> str:
        """Convert this history to JSON.

        Note, this does not include the workflow ID.
        """
        return google.protobuf.json_format.MessageToJson(
            temporalio.api.history.v1.History(events=self.events)
        )

    def to_json_dict(self) -> dict[str, Any]:
        """Convert this history to JSON-compatible dict.

        Note, this does not include the workflow ID.
        """
        return google.protobuf.json_format.MessageToDict(
            temporalio.api.history.v1.History(events=self.events)
        )


@dataclass
class WorkflowHistoryEventAsyncIterator:
    """Asynchronous iterator for history events of a workflow.

    Most users should use ``async for`` on this iterator and not call any of the
    methods within.
    """

    def __init__(
        self,
        client: Client,
        input: FetchWorkflowHistoryEventsInput,
    ) -> None:
        """Create an asynchronous iterator for the given input.

        Users should not create this directly, but rather use
        :py:meth:`WorkflowHandle.fetch_history_events`.
        """
        self._client = client
        self._input = input
        self._next_page_token = input.next_page_token
        self._current_page: (
            None | (Sequence[temporalio.api.history.v1.HistoryEvent])
        ) = None
        self._current_page_index = 0

    @property
    def current_page_index(self) -> int:
        """Index of the entry in the current page that will be returned from
        the next :py:meth:`__anext__` call.
        """
        return self._current_page_index

    @property
    def current_page(
        self,
    ) -> Sequence[temporalio.api.history.v1.HistoryEvent] | None:
        """Current page, if it has been fetched yet."""
        return self._current_page

    @property
    def next_page_token(self) -> bytes | None:
        """Token for the next page request if any."""
        return self._next_page_token

    async def fetch_next_page(self, *, page_size: int | None = None) -> None:  # type:ignore[reportUnusedParameter] # https://github.com/temporalio/sdk-python/issues/1239
        """Fetch the next page if any.

        Args:
            page_size: Override the page size this iterator was originally
                created with.
        """
        resp = await self._client.workflow_service.get_workflow_execution_history(
            temporalio.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest(
                namespace=self._client.namespace,
                execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=self._input.id,
                    run_id=self._input.run_id or "",
                ),
                maximum_page_size=page_size or self._input.page_size or 0,
                next_page_token=self._next_page_token or b"",
                wait_new_event=self._input.wait_new_event,
                history_event_filter_type=temporalio.api.enums.v1.HistoryEventFilterType.ValueType(
                    self._input.event_filter_type
                ),
                skip_archival=self._input.skip_archival,
            ),
            retry=True,
            metadata=self._input.rpc_metadata,
            timeout=self._input.rpc_timeout,
        )
        # We don't support raw history
        assert len(resp.raw_history) == 0
        self._current_page = list(resp.history.events)
        self._current_page_index = 0
        self._next_page_token = resp.next_page_token or None

    def __aiter__(self) -> WorkflowHistoryEventAsyncIterator:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> temporalio.api.history.v1.HistoryEvent:
        """Get the next execution on this iterator, fetching next page if
        necessary.
        """
        while True:
            # No page? fetch and continue
            if self._current_page is None:
                await self.fetch_next_page()
                continue
            # No more left in page?
            if self._current_page_index >= len(self._current_page):
                # If there is a next page token, try to get another page and try
                # again
                if self._next_page_token is not None:
                    await self.fetch_next_page()
                    continue
                # No more pages means we're done
                raise StopAsyncIteration
            # Increment page index and return
            ret = self._current_page[self._current_page_index]
            self._current_page_index += 1
            return ret


class WorkflowUpdateHandle(Generic[LocalReturnType]):
    """Handle for a workflow update execution request."""

    def __init__(
        self,
        client: Client,
        id: str,
        workflow_id: str,
        *,
        workflow_run_id: str | None = None,
        result_type: type | None = None,
        known_outcome: temporalio.api.update.v1.Outcome | None = None,
    ):
        """Create a workflow update handle.

        Users should not create this directly, but rather use
        :py:meth:`WorkflowHandle.start_update` or :py:meth:`WorkflowHandle.get_update_handle`.
        """
        self._client = client
        self._id = id
        self._workflow_id = workflow_id
        self._workflow_run_id = workflow_run_id
        self._result_type = result_type
        self._known_outcome = known_outcome

    @functools.cached_property
    def _data_converter(self) -> temporalio.converter.DataConverter:
        return self._client.data_converter.with_context(
            WorkflowSerializationContext(
                namespace=self._client.namespace,
                workflow_id=self.workflow_id,
            )
        )

    @property
    def id(self) -> str:
        """ID of this Update request."""
        return self._id

    @property
    def workflow_id(self) -> str:
        """The ID of the Workflow targeted by this Update."""
        return self._workflow_id

    @property
    def workflow_run_id(self) -> str | None:
        """If specified, the specific run of the Workflow targeted by this Update."""
        return self._workflow_run_id

    async def result(
        self,
        *,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> LocalReturnType:
        """Wait for and return the result of the update. The result may already be known in which case no network call
        is made. Otherwise the result will be polled for until it is returned.

        Args:
            rpc_metadata: Headers used on the RPC call. Keys here override client-level RPC metadata keys.
            rpc_timeout: Optional RPC deadline to set for each RPC call. Note: this is the timeout for each
                RPC call while polling, not a timeout for the function as a whole. If an individual RPC times out,
                it will be retried until the result is available.

        Raises:
            WorkflowUpdateFailedError: If the update failed.
            WorkflowUpdateRPCTimeoutOrCancelledError: This update call timed out
                or was cancelled. This doesn't mean the update itself was timed
                out or cancelled.
            RPCError: Update result could not be fetched for some other reason.
        """
        # Poll until outcome reached
        await self._poll_until_outcome(
            rpc_metadata=rpc_metadata, rpc_timeout=rpc_timeout
        )

        # Convert outcome to failure or value
        assert self._known_outcome
        if self._known_outcome.HasField("failure"):
            raise WorkflowUpdateFailedError(
                await self._data_converter.decode_failure(self._known_outcome.failure),
            )
        if not self._known_outcome.success.payloads:
            return None  # type: ignore
        type_hints = [self._result_type] if self._result_type else None
        results = await self._data_converter.decode(
            self._known_outcome.success.payloads, type_hints
        )
        if not results:
            return None  # type: ignore
        elif len(results) > 1:
            warnings.warn(f"Expected single update result, got {len(results)}")
        return results[0]

    async def _poll_until_outcome(
        self,
        rpc_metadata: Mapping[str, str | bytes] = {},
        rpc_timeout: timedelta | None = None,
    ) -> None:
        if self._known_outcome:
            return
        req = temporalio.api.workflowservice.v1.PollWorkflowExecutionUpdateRequest(
            namespace=self._client.namespace,
            update_ref=temporalio.api.update.v1.UpdateRef(
                workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                    workflow_id=self.workflow_id,
                    run_id=self.workflow_run_id or "",
                ),
                update_id=self.id,
            ),
            identity=self._client.identity,
            wait_policy=temporalio.api.update.v1.WaitPolicy(
                lifecycle_stage=temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
            ),
        )

        # Continue polling as long as we have no outcome
        while True:
            try:
                res = (
                    await self._client.workflow_service.poll_workflow_execution_update(
                        req,
                        retry=True,
                        metadata=rpc_metadata,
                        timeout=rpc_timeout,
                    )
                )
                if res.HasField("outcome"):
                    self._known_outcome = res.outcome
                    return
            except RPCError as err:
                if (
                    err.status == RPCStatusCode.DEADLINE_EXCEEDED
                    or err.status == RPCStatusCode.CANCELLED
                ):
                    raise WorkflowUpdateRPCTimeoutOrCancelledError() from err
                else:
                    raise
            except asyncio.CancelledError as err:
                raise WorkflowUpdateRPCTimeoutOrCancelledError() from err


class WorkflowUpdateStage(IntEnum):
    """Stage to wait for workflow update to reach before returning from
    ``start_update``.
    """

    ADMITTED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED
    )
    ACCEPTED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
    )
    COMPLETED = int(
        temporalio.api.enums.v1.UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
    )
