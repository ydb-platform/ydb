"""Workflow worker runner and instance."""

from __future__ import annotations

import asyncio
import collections
import contextvars
import inspect
import json
import logging
import random
import sys
import threading
import traceback
import warnings
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import nexusrpc.handler
from nexusrpc import InputT, OutputT
from typing_extensions import Self, TypeAlias, TypedDict

import temporalio.activity
import temporalio.api.common.v1
import temporalio.api.enums.v1
import temporalio.api.sdk.v1
import temporalio.bridge.proto.activity_result
import temporalio.bridge.proto.child_workflow
import temporalio.bridge.proto.common
import temporalio.bridge.proto.nexus
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_commands
import temporalio.bridge.proto.workflow_completion
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.workflow
from temporalio.service import __version__

from ._interceptor import (
    ContinueAsNewInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    StartNexusOperationInput,
    WorkflowInboundInterceptor,
    WorkflowOutboundInterceptor,
)

logger = logging.getLogger(__name__)

# Set to true to log all cases where we're ignoring things during delete
LOG_IGNORE_DURING_DELETE = False


class WorkflowRunner(ABC):
    """Abstract runner for workflows that creates workflow instances to run.

    :py:class:`UnsandboxedWorkflowRunner` is an implementation that locally runs
    the workflow.
    """

    @abstractmethod
    def prepare_workflow(self, defn: temporalio.workflow._Definition) -> None:
        """Prepare a workflow for future execution.

        This is run once for each workflow definition when a worker starts. This
        allows the runner to do anything necessarily to prepare for this
        definition to be used multiple times in create_instance.

        Args:
            defn: The workflow definition.
        """
        raise NotImplementedError

    @abstractmethod
    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a workflow instance that can handle activations.

        Args:
            det: Details that can be used to create the instance.

        Returns:
            Workflow instance that can handle activations.
        """
        raise NotImplementedError

    def set_worker_level_failure_exception_types(
        self, types: Sequence[Type[BaseException]]
    ) -> None:
        """Set worker-level failure exception types that will be used to
        validate in the sandbox when calling ``prepare_workflow``.

        Args:
            types: Exception types.
        """
        pass


@dataclass(frozen=True)
class WorkflowInstanceDetails:
    """Immutable details for creating a workflow instance."""

    payload_converter_class: Type[temporalio.converter.PayloadConverter]
    failure_converter_class: Type[temporalio.converter.FailureConverter]
    interceptor_classes: Sequence[Type[WorkflowInboundInterceptor]]
    defn: temporalio.workflow._Definition
    info: temporalio.workflow.Info
    randomness_seed: int
    extern_functions: Mapping[str, Callable]
    disable_eager_activity_execution: bool
    worker_level_failure_exception_types: Sequence[Type[BaseException]]


class WorkflowInstance(ABC):
    """Instance of a workflow that can handle activations."""

    @abstractmethod
    def activate(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        """Handle an activation and return completion.

        This should never raise an exception, but instead catch all exceptions
        and set as completion failure.

        Args:
            act: Protobuf activation.

        Returns:
            Completion object with successful commands set or failure info set.
        """
        raise NotImplementedError

    def get_thread_id(self) -> Optional[int]:
        """Return the thread identifier that this workflow is running on.

        Not an abstractmethod because it is not mandatory to implement. Used primarily for getting the frames of a deadlocked thread.

        Returns:
            Thread ID if the workflow is running, None if not.
        """
        return None


class UnsandboxedWorkflowRunner(WorkflowRunner):
    """Workflow runner that does not do any sandboxing."""

    def prepare_workflow(self, defn: temporalio.workflow._Definition) -> None:
        """Implements :py:meth:`WorkflowRunner.prepare_workflow` as a no-op."""
        pass

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Implements :py:meth:`WorkflowRunner.create_instance`."""
        # We ignore MyPy failing to instantiate this because it's not _really_
        # abstract at runtime. All of the asyncio.AbstractEventLoop calls that
        # we are not implementing are not abstract, they just throw not-impl'd
        # errors.
        return _WorkflowInstanceImpl(det)  # type: ignore[abstract]


_T = TypeVar("_T")
_Context: TypeAlias = Dict[str, Any]
_ExceptionHandler: TypeAlias = Callable[[asyncio.AbstractEventLoop, _Context], Any]


class _WorkflowInstanceImpl(  # type: ignore[reportImplicitAbstractClass]
    WorkflowInstance, temporalio.workflow._Runtime, asyncio.AbstractEventLoop
):
    def __init__(self, det: WorkflowInstanceDetails) -> None:
        # No init for AbstractEventLoop
        WorkflowInstance.__init__(self)
        temporalio.workflow._Runtime.__init__(self)
        self._payload_converter = det.payload_converter_class()
        self._failure_converter = det.failure_converter_class()
        self._defn = det.defn
        self._workflow_input: Optional[ExecuteWorkflowInput] = None
        self._info = det.info
        self._extern_functions = det.extern_functions
        self._disable_eager_activity_execution = det.disable_eager_activity_execution
        self._worker_level_failure_exception_types = (
            det.worker_level_failure_exception_types
        )
        self._primary_task: Optional[asyncio.Task[None]] = None
        self._time_ns = 0
        self._cancel_requested = False
        self._deployment_version_for_current_task: Optional[
            temporalio.bridge.proto.common.WorkerDeploymentVersion
        ] = None
        self._current_history_length = 0
        self._current_history_size = 0
        self._continue_as_new_suggested = False
        # Lazily loaded
        self._untyped_converted_memo: Optional[MutableMapping[str, Any]] = None
        # Handles which are ready to run on the next event loop iteration
        self._ready: Deque[asyncio.Handle] = collections.deque()
        self._conditions: List[Tuple[Callable[[], bool], asyncio.Future]] = []
        # Keyed by seq
        self._pending_timers: Dict[int, _TimerHandle] = {}
        self._pending_activities: Dict[int, _ActivityHandle] = {}
        self._pending_child_workflows: Dict[int, _ChildWorkflowHandle] = {}
        self._pending_nexus_operations: Dict[int, _NexusOperationHandle] = {}
        self._pending_external_signals: Dict[int, asyncio.Future] = {}
        self._pending_external_cancels: Dict[int, asyncio.Future] = {}
        # Keyed by type
        self._curr_seqs: Dict[str, int] = {}
        # TODO(cretz): Any concerns about not sharing this? Maybe the types I
        # need to lookup should be done at definition time?
        self._exception_handler: Optional[_ExceptionHandler] = None
        # The actual instance, instantiated on first _run_once
        self._object: Any = None
        self._is_replaying: bool = False
        self._random = random.Random(det.randomness_seed)
        self._read_only = False

        # Patches we have been notified of and memoized patch responses
        self._patches_notified: Set[str] = set()
        self._patches_memoized: Dict[str, bool] = {}

        # Tasks stored by asyncio are weak references and therefore can get GC'd
        # which can cause warnings like "Task was destroyed but it is pending!".
        # So we store the tasks ourselves.
        # See https://bugs.python.org/issue21163 and others.
        self._tasks: Set[asyncio.Task] = set()

        # We maintain signals, queries, and updates on this class since handlers can be
        # added during workflow execution
        self._signals = dict(self._defn.signals)
        self._queries = dict(self._defn.queries)
        self._updates = dict(self._defn.updates)

        # We record in-progress signals and updates in order to support waiting for handlers to
        # finish, and issuing warnings when the workflow exits with unfinished handlers. Since
        # signals lack a unique per-invocation identifier, we introduce a sequence number for the
        # purpose.
        self._handled_signals_seq = 0
        self._in_progress_signals: Dict[int, HandlerExecution] = {}
        self._in_progress_updates: Dict[str, HandlerExecution] = {}

        # Add stack trace handler
        # TODO(cretz): Is it ok that this can be forcefully overridden by the
        # workflow author? They could technically override in interceptor
        # anyways. Putting here ensures it ends up in the query list on
        # query-not-found error.
        self._queries["__stack_trace"] = temporalio.workflow._QueryDefinition(
            name="__stack_trace",
            fn=self._stack_trace,
            is_method=False,
            arg_types=[],
            ret_type=str,
        )

        self._queries["__enhanced_stack_trace"] = temporalio.workflow._QueryDefinition(
            name="__enhanced_stack_trace",
            fn=self._enhanced_stack_trace,
            is_method=False,
            arg_types=[],
            ret_type=temporalio.api.sdk.v1.EnhancedStackTrace,
        )

        self._queries["__temporal_workflow_metadata"] = (
            temporalio.workflow._QueryDefinition(
                name="__temporal_workflow_metadata",
                fn=self._temporal_workflow_metadata,
                is_method=False,
                arg_types=[],
                ret_type=temporalio.api.sdk.v1.WorkflowMetadata,
            )
        )

        # Maintain buffered signals for later-added dynamic handlers
        self._buffered_signals: Dict[
            str, List[temporalio.bridge.proto.workflow_activation.SignalWorkflow]
        ] = {}

        # When we evict, we have to mark the workflow as deleting so we don't
        # add any commands and we swallow exceptions on tear down
        self._deleting = False

        # We only create the metric meter lazily
        self._metric_meter: Optional[_ReplaySafeMetricMeter] = None

        # For tracking the thread this workflow is running on (primarily for deadlock situations)
        self._current_thread_id: Optional[int] = None

        # The current details (as opposed to static details on workflow start), returned in the
        # metadata query
        self._current_details = ""

        # The versioning behavior of this workflow, as established by annotation or by the dynamic
        # config function. Is only set once upon initialization.
        self._versioning_behavior: Optional[temporalio.common.VersioningBehavior] = None

        # Dynamic failure exception types as overridden by the dynamic config function
        self._dynamic_failure_exception_types: Optional[
            Sequence[type[BaseException]]
        ] = None

        # Create interceptors. We do this with our runtime on the loop just in
        # case they want to access info() during init(). This should remain at the end of the constructor so that variables are defined during interceptor creation
        temporalio.workflow._Runtime.set_on_loop(asyncio.get_running_loop(), self)
        try:
            root_inbound = _WorkflowInboundImpl(self)
            self._inbound: WorkflowInboundInterceptor = root_inbound
            for interceptor_class in reversed(list(det.interceptor_classes)):
                self._inbound = interceptor_class(self._inbound)
            # During init we set ourselves on the current loop
            self._inbound.init(_WorkflowOutboundImpl(self))
            self._outbound = root_inbound._outbound
        finally:
            # Remove our runtime from the loop
            temporalio.workflow._Runtime.set_on_loop(asyncio.get_running_loop(), None)

        # Set ourselves on our own loop
        temporalio.workflow._Runtime.set_on_loop(self, self)

    def get_thread_id(self) -> Optional[int]:
        return self._current_thread_id

    #### Activation functions ####
    # These are in alphabetical order and besides "activate", and
    # "_make_workflow_input", all other calls are "_apply_" + the job field
    # name.

    def activate(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        # Reset current completion, time, and whether replaying
        self._current_completion = (
            temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion()
        )
        self._current_completion.successful.SetInParent()

        self._current_activation_error: Optional[Exception] = None
        self._deployment_version_for_current_task = (
            act.deployment_version_for_current_task
        )
        self._current_history_length = act.history_length
        self._current_history_size = act.history_size_bytes
        self._continue_as_new_suggested = act.continue_as_new_suggested
        self._time_ns = act.timestamp.ToNanoseconds()
        self._is_replaying = act.is_replaying
        self._current_thread_id = threading.get_ident()
        self._current_internal_flags = act.available_internal_flags
        activation_err: Optional[Exception] = None
        try:
            # Split into job sets with patches, then signals + updates, then
            # non-queries, then queries
            start_job = None
            job_sets: List[
                List[temporalio.bridge.proto.workflow_activation.WorkflowActivationJob]
            ] = [[], [], [], []]
            for job in act.jobs:
                if job.HasField("notify_has_patch"):
                    job_sets[0].append(job)
                elif job.HasField("signal_workflow") or job.HasField("do_update"):
                    job_sets[1].append(job)
                elif not job.HasField("query_workflow"):
                    if job.HasField("initialize_workflow"):
                        start_job = job.initialize_workflow
                    job_sets[2].append(job)
                else:
                    job_sets[3].append(job)

            if start_job:
                self._workflow_input = self._make_workflow_input(start_job)

            # Apply every job set, running after each set
            for index, job_set in enumerate(job_sets):
                if not job_set:
                    continue
                for job in job_set:
                    # Let errors bubble out of these to the caller to fail the task
                    self._apply(job)

                # Run one iteration of the loop. We do not allow conditions to
                # be checked in patch jobs (first index) or query jobs (last
                # index).
                self._run_once(check_conditions=index == 1 or index == 2)
        except Exception as err:
            # We want some errors during activation, like those that can happen
            # during payload conversion, to be able to fail the workflow not the
            # task
            if self._is_workflow_failure_exception(err):
                try:
                    self._set_workflow_failure(err)
                except Exception as inner_err:
                    activation_err = inner_err
            else:
                # Otherwise all exceptions are activation errors
                activation_err = err
            # If we're deleting, swallow any activation error
            if self._deleting:
                if LOG_IGNORE_DURING_DELETE:
                    logger.debug(
                        "Ignoring exception while deleting workflow", exc_info=True
                    )
                activation_err = None

        # Apply versioning behavior if one was established
        if self._versioning_behavior:
            self._current_completion.successful.versioning_behavior = (
                self._versioning_behavior.value
            )

        # If we're deleting, there better be no more tasks. It is important for
        # the integrity of the system that we check this. If there are tasks
        # remaining, they and any associated coroutines will get garbage
        # collected which can trigger a GeneratorExit exception thrown in the
        # coroutine which can cause it to wakeup on a different thread which may
        # have a different workflow/event-loop going.
        if self._deleting and self._tasks:
            raise RuntimeError(
                f"Eviction processed, but {len(self._tasks)} tasks remain. "
                + f"Stack traces below:\n\n{self._stack_trace()}"
            )

        if activation_err:
            logger.warning(
                f"Failed activation on workflow {self._info.workflow_type} with ID {self._info.workflow_id} and run ID {self._info.run_id}",
                exc_info=activation_err,
                extra={
                    "temporal_workflow": self._info._logger_details(),
                    "__temporal_error_identifier": "WorkflowTaskFailure",
                },
            )
            # Set completion failure
            self._current_completion.failed.failure.SetInParent()
            try:
                self._failure_converter.to_failure(
                    activation_err,
                    self._payload_converter,
                    self._current_completion.failed.failure,
                )
            except Exception as inner_err:
                logger.exception(
                    f"Failed converting activation exception on workflow with run ID {act.run_id}"
                )
                self._current_completion.failed.failure.message = (
                    f"Failed converting activation exception: {inner_err}"
                )
                self._current_completion.failed.failure.application_failure_info.SetInParent()

        def is_completion(command):
            return (
                command.HasField("complete_workflow_execution")
                or command.HasField("continue_as_new_workflow_execution")
                or command.HasField("fail_workflow_execution")
                or command.HasField("cancel_workflow_execution")
            )

        if any(map(is_completion, self._current_completion.successful.commands)):
            self._warn_if_unfinished_handlers()

        return self._current_completion

    def _apply(
        self, job: temporalio.bridge.proto.workflow_activation.WorkflowActivationJob
    ) -> None:
        if job.HasField("cancel_workflow"):
            self._apply_cancel_workflow(job.cancel_workflow)
        elif job.HasField("do_update"):
            self._apply_do_update(job.do_update)
        elif job.HasField("fire_timer"):
            self._apply_fire_timer(job.fire_timer)
        elif job.HasField("query_workflow"):
            self._apply_query_workflow(job.query_workflow)
        elif job.HasField("notify_has_patch"):
            self._apply_notify_has_patch(job.notify_has_patch)
        elif job.HasField("remove_from_cache"):
            self._apply_remove_from_cache(job.remove_from_cache)
        elif job.HasField("resolve_activity"):
            self._apply_resolve_activity(job.resolve_activity)
        elif job.HasField("resolve_child_workflow_execution"):
            self._apply_resolve_child_workflow_execution(
                job.resolve_child_workflow_execution
            )
        elif job.HasField("resolve_child_workflow_execution_start"):
            self._apply_resolve_child_workflow_execution_start(
                job.resolve_child_workflow_execution_start
            )
        elif job.HasField("resolve_nexus_operation_start"):
            self._apply_resolve_nexus_operation_start(job.resolve_nexus_operation_start)
        elif job.HasField("resolve_nexus_operation"):
            self._apply_resolve_nexus_operation(job.resolve_nexus_operation)
        elif job.HasField("resolve_request_cancel_external_workflow"):
            self._apply_resolve_request_cancel_external_workflow(
                job.resolve_request_cancel_external_workflow
            )
        elif job.HasField("resolve_signal_external_workflow"):
            self._apply_resolve_signal_external_workflow(
                job.resolve_signal_external_workflow
            )
        elif job.HasField("signal_workflow"):
            self._apply_signal_workflow(job.signal_workflow)
        elif job.HasField("initialize_workflow"):
            self._apply_initialize_workflow(job.initialize_workflow)
        elif job.HasField("update_random_seed"):
            self._apply_update_random_seed(job.update_random_seed)
        else:
            raise RuntimeError(f"Unrecognized job: {job.WhichOneof('variant')}")

    def _apply_cancel_workflow(
        self, job: temporalio.bridge.proto.workflow_activation.CancelWorkflow
    ) -> None:
        self._cancel_requested = True
        # TODO(cretz): Details or cancel message or whatever?
        if self._primary_task:
            # The primary task may not have started yet and we want to give the
            # workflow the ability to receive the cancellation, so we must defer
            # this cancellation to the next iteration of the event loop.
            self.call_soon(self._primary_task.cancel)

    def _apply_do_update(
        self, job: temporalio.bridge.proto.workflow_activation.DoUpdate
    ):
        # Run the validator & handler in a task. Everything, including looking up the update definition, needs to be
        # inside the task, since the update may not be defined until after we have started the workflow - for example
        # if an update is in the first WFT & is also registered dynamically at the top of workflow code.
        async def run_update() -> None:
            # Set the current update for the life of this task
            temporalio.workflow._set_current_update_info(
                temporalio.workflow.UpdateInfo(id=job.id, name=job.name)
            )

            command = self._add_command()
            command.update_response.protocol_instance_id = job.protocol_instance_id
            past_validation = False
            try:
                defn = self._updates.get(job.name) or self._updates.get(None)
                if not defn:
                    known_updates = sorted([k for k in self._updates.keys() if k])
                    raise RuntimeError(
                        f"Update handler for '{job.name}' expected but not found, and there is no dynamic handler. "
                        f"known updates: [{' '.join(known_updates)}]"
                    )
                self._in_progress_updates[job.id] = HandlerExecution(
                    job.name, defn.unfinished_policy, job.id
                )
                args = self._process_handler_args(
                    job.name,
                    job.input,
                    defn.name,
                    defn.arg_types,
                    defn.dynamic_vararg,
                )
                handler_input = HandleUpdateInput(
                    id=job.id,
                    update=job.name,
                    args=args,
                    headers=job.headers,
                )

                if job.run_validator and defn.validator is not None:
                    with self._as_read_only():
                        self._inbound.handle_update_validator(handler_input)
                        # Re-process arguments to avoid any problems caused by user mutation of them during validation
                        args = self._process_handler_args(
                            job.name,
                            job.input,
                            defn.name,
                            defn.arg_types,
                            defn.dynamic_vararg,
                        )
                        handler_input.args = args

                past_validation = True
                # Accept the update
                command.update_response.accepted.SetInParent()
                command = None  # type: ignore

                # Run the handler
                success = await self._inbound.handle_update_handler(handler_input)
                result_payloads = self._payload_converter.to_payloads([success])
                if len(result_payloads) != 1:
                    raise ValueError(
                        f"Expected 1 result payload, got {len(result_payloads)}"
                    )
                command = self._add_command()
                command.update_response.protocol_instance_id = job.protocol_instance_id
                command.update_response.completed.CopyFrom(result_payloads[0])
            except (Exception, asyncio.CancelledError) as err:
                logger.debug(
                    f"Update raised failure with run ID {self._info.run_id}",
                    exc_info=True,
                )
                # All asyncio cancelled errors become Temporal cancelled errors
                if isinstance(err, asyncio.CancelledError):
                    err = temporalio.exceptions.CancelledError(
                        f"Cancellation raised within update {err}"
                    )
                # Read-only issues during validation should fail the task
                if isinstance(err, temporalio.workflow.ReadOnlyContextError):
                    self._current_activation_error = err
                    return
                # Validation failures are always update failures. We reuse
                # workflow failure logic to decide task failure vs update
                # failure after validation.
                if not past_validation or self._is_workflow_failure_exception(err):
                    if command is None:
                        command = self._add_command()
                        command.update_response.protocol_instance_id = (
                            job.protocol_instance_id
                        )
                    command.update_response.rejected.SetInParent()
                    self._failure_converter.to_failure(
                        err,
                        self._payload_converter,
                        command.update_response.rejected,
                    )
                else:
                    self._current_activation_error = err
                    return
            except BaseException:
                if self._deleting:
                    if LOG_IGNORE_DURING_DELETE:
                        logger.debug(
                            "Ignoring exception while deleting workflow", exc_info=True
                        )
                    return
                raise
            finally:
                self._in_progress_updates.pop(job.id, None)

        self.create_task(
            run_update(),
            name=f"update: {job.name}",
        )

    def _apply_fire_timer(
        self, job: temporalio.bridge.proto.workflow_activation.FireTimer
    ) -> None:
        # We ignore an absent handler because it may have been cancelled and
        # removed earlier this activation by a signal
        handle = self._pending_timers.pop(job.seq, None)
        if handle:
            self._ready.append(handle)

    def _apply_query_workflow(
        self, job: temporalio.bridge.proto.workflow_activation.QueryWorkflow
    ) -> None:
        # Wrap entire bunch of work in a task
        async def run_query() -> None:
            try:
                with self._as_read_only():
                    # Named query or dynamic
                    defn = self._queries.get(job.query_type) or self._queries.get(None)
                    if not defn:
                        known_queries = sorted([k for k in self._queries.keys() if k])
                        raise RuntimeError(
                            f"Query handler for '{job.query_type}' expected but not found, "
                            f"known queries: [{' '.join(known_queries)}]"
                        )

                    # Create input
                    args = self._process_handler_args(
                        job.query_type,
                        job.arguments,
                        defn.name,
                        defn.arg_types,
                        defn.dynamic_vararg,
                    )
                    input = HandleQueryInput(
                        id=job.query_id,
                        query=job.query_type,
                        args=args,
                        headers=job.headers,
                    )
                    success = await self._inbound.handle_query(input)
                    result_payloads = self._payload_converter.to_payloads([success])
                    if len(result_payloads) != 1:
                        raise ValueError(
                            f"Expected 1 result payload, got {len(result_payloads)}"
                        )
                command = self._add_command()
                command.respond_to_query.query_id = job.query_id
                command.respond_to_query.succeeded.response.CopyFrom(result_payloads[0])
            except Exception as err:
                try:
                    command = self._add_command()
                    command.respond_to_query.query_id = job.query_id
                    self._failure_converter.to_failure(
                        err,
                        self._payload_converter,
                        command.respond_to_query.failed,
                    )
                except Exception as inner_err:
                    raise ValueError(
                        "Failed converting application error"
                    ) from inner_err

        # Schedule it
        self.create_task(run_query(), name=f"query: {job.query_type}")

    def _apply_notify_has_patch(
        self, job: temporalio.bridge.proto.workflow_activation.NotifyHasPatch
    ) -> None:
        self._patches_notified.add(job.patch_id)

    def _apply_remove_from_cache(
        self, job: temporalio.bridge.proto.workflow_activation.RemoveFromCache
    ) -> None:
        self._deleting = True
        self._cancel_requested = True
        # We consider eviction to be under replay so that certain code like
        # logging that avoids replaying doesn't run during eviction either
        self._is_replaying = True
        # Cancel everything
        for task in self._tasks:
            task.cancel()

    def _apply_resolve_activity(
        self, job: temporalio.bridge.proto.workflow_activation.ResolveActivity
    ) -> None:
        handle = self._pending_activities.pop(job.seq, None)
        if not handle:
            raise RuntimeError(f"Failed finding activity handle for sequence {job.seq}")
        if job.result.HasField("completed"):
            ret: Optional[Any] = None
            if job.result.completed.HasField("result"):
                ret_types = [handle._input.ret_type] if handle._input.ret_type else None
                ret_vals = self._convert_payloads(
                    [job.result.completed.result],
                    ret_types,
                )
                ret = ret_vals[0]
            handle._resolve_success(ret)
        elif job.result.HasField("failed"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.result.failed.failure, self._payload_converter
                )
            )
        elif job.result.HasField("cancelled"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.result.cancelled.failure, self._payload_converter
                )
            )
        elif job.result.HasField("backoff"):
            handle._resolve_backoff(job.result.backoff)
        else:
            raise RuntimeError("Activity did not have result")

    def _apply_resolve_child_workflow_execution(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveChildWorkflowExecution,
    ) -> None:
        handle = self._pending_child_workflows.pop(job.seq, None)
        if not handle:
            raise RuntimeError(
                f"Failed finding child workflow handle for sequence {job.seq}"
            )
        if job.result.HasField("completed"):
            ret: Optional[Any] = None
            if job.result.completed.HasField("result"):
                ret_types = [handle._input.ret_type] if handle._input.ret_type else None
                ret_vals = self._convert_payloads(
                    [job.result.completed.result],
                    ret_types,
                )
                ret = ret_vals[0]
            handle._resolve_success(ret)
        elif job.result.HasField("failed"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.result.failed.failure, self._payload_converter
                )
            )
        elif job.result.HasField("cancelled"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.result.cancelled.failure, self._payload_converter
                )
            )
        else:
            raise RuntimeError("Child workflow did not have result")

    def _apply_resolve_child_workflow_execution_start(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveChildWorkflowExecutionStart,
    ) -> None:
        handle = self._pending_child_workflows.get(job.seq)
        if not handle:
            raise RuntimeError(
                f"Failed finding child workflow handle for sequence {job.seq}"
            )
        if job.HasField("succeeded"):
            # We intentionally do not pop here because this same handle is used
            # for waiting on the entire workflow to complete
            handle._resolve_start_success(job.succeeded.run_id)
        elif job.HasField("failed"):
            self._pending_child_workflows.pop(job.seq)
            if (
                job.failed.cause
                == temporalio.bridge.proto.child_workflow.StartChildWorkflowExecutionFailedCause.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
            ):
                handle._resolve_failure(
                    temporalio.exceptions.WorkflowAlreadyStartedError(
                        job.failed.workflow_id, job.failed.workflow_type
                    )
                )
            else:
                handle._resolve_failure(
                    RuntimeError(f"Unknown child start fail cause: {job.failed.cause}")
                )
        elif job.HasField("cancelled"):
            self._pending_child_workflows.pop(job.seq)
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.cancelled.failure, self._payload_converter
                )
            )
        else:
            raise RuntimeError("Child workflow start did not have a known status")

    def _apply_resolve_nexus_operation_start(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveNexusOperationStart,
    ) -> None:
        handle = self._pending_nexus_operations.get(job.seq)
        if not handle:
            raise RuntimeError(
                f"Failed to find nexus operation handle for job sequence number {job.seq}"
            )
        if job.HasField("operation_token"):
            # The nexus operation started asynchronously. A `ResolveNexusOperation` job
            # will follow in a future activation.
            handle._resolve_start_success(job.operation_token)
        elif job.HasField("started_sync"):
            # The nexus operation 'started' in the sense that it's already resolved. A
            # `ResolveNexusOperation` job will be in the same activation.
            handle._resolve_start_success(None)
        elif job.HasField("failed"):
            # The nexus operation start failed; no ResolveNexusOperation will follow.
            self._pending_nexus_operations.pop(job.seq, None)
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    job.failed, self._payload_converter
                )
            )
        else:
            raise ValueError(f"Unknown Nexus operation start status: {job}")

    def _apply_resolve_nexus_operation(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveNexusOperation,
    ) -> None:
        handle = self._pending_nexus_operations.pop(job.seq, None)
        if not handle:
            # One way this can occur is:
            # 1. Cancel request issued with cancellation_type=WaitRequested.
            # 2. Server receives nexus cancel handler task completion and writes
            #    NexusOperationCancelRequestCompleted / NexusOperationCancelRequestFailed. On
            #    consuming this event, core sends an activation resolving the handle future as
            #    completed / failed.
            # 4. Subsequently, the nexus operation completes as completed/failed, causing the server
            #    to write NexusOperationCompleted / NexusOperationFailed. On consuming this event,
            #    core sends an activation which would attempt to resolve the handle future as
            #    completed / failed, but it has already been resolved.
            return

        # Handle the four oneof variants of NexusOperationResult
        result = job.result
        if result.HasField("completed"):
            [output] = self._convert_payloads(
                [result.completed],
                [handle._input.output_type] if handle._input.output_type else None,
            )
            handle._resolve_success(output)
        elif result.HasField("failed"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    result.failed, self._payload_converter
                )
            )
        elif result.HasField("cancelled"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    result.cancelled, self._payload_converter
                )
            )
        elif result.HasField("timed_out"):
            handle._resolve_failure(
                self._failure_converter.from_failure(
                    result.timed_out, self._payload_converter
                )
            )
        else:
            raise RuntimeError("Nexus operation did not have a result")

    def _apply_resolve_request_cancel_external_workflow(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveRequestCancelExternalWorkflow,
    ) -> None:
        fut = self._pending_external_cancels.pop(job.seq, None)
        if not fut:
            raise RuntimeError(
                f"Failed finding pending external cancel for sequence {job.seq}"
            )
        # We intentionally let this error if future is already done
        if job.HasField("failure"):
            fut.set_exception(
                self._failure_converter.from_failure(
                    job.failure, self._payload_converter
                )
            )
        else:
            fut.set_result(None)

    def _apply_resolve_signal_external_workflow(
        self,
        job: temporalio.bridge.proto.workflow_activation.ResolveSignalExternalWorkflow,
    ) -> None:
        fut = self._pending_external_signals.pop(job.seq, None)
        if not fut:
            raise RuntimeError(
                f"Failed finding pending external signal for sequence {job.seq}"
            )
        # We intentionally let this error if future is already done
        if job.HasField("failure"):
            fut.set_exception(
                self._failure_converter.from_failure(
                    job.failure, self._payload_converter
                )
            )
        else:
            fut.set_result(None)

    def _apply_signal_workflow(
        self, job: temporalio.bridge.proto.workflow_activation.SignalWorkflow
    ) -> None:
        # Apply to named or to dynamic or buffer
        signal_defn = self._signals.get(job.signal_name) or self._signals.get(None)
        if not signal_defn:
            self._buffered_signals.setdefault(job.signal_name, []).append(job)
            return
        self._process_signal_job(signal_defn, job)

    def _apply_initialize_workflow(
        self, job: temporalio.bridge.proto.workflow_activation.InitializeWorkflow
    ) -> None:
        # Async call to run on the scheduler thread. This will be wrapped in
        # another function which applies exception handling.
        async def run_workflow(input: ExecuteWorkflowInput) -> None:
            try:
                result = await self._inbound.execute_workflow(input)
                result_payloads = self._payload_converter.to_payloads([result])
                if len(result_payloads) != 1:
                    raise ValueError(
                        f"Expected 1 result payload, got {len(result_payloads)}"
                    )
                command = self._add_command()
                command.complete_workflow_execution.result.CopyFrom(result_payloads[0])
            except Exception:
                if self._deleting:
                    if LOG_IGNORE_DURING_DELETE:
                        logger.debug(
                            "Ignoring exception while deleting workflow", exc_info=True
                        )
                    return
                raise

        if not self._workflow_input:
            raise RuntimeError(
                "Expected workflow input to be set. This is an SDK Python bug."
            )
        self._primary_task = self.create_task(
            self._run_top_level_workflow_function(run_workflow(self._workflow_input)),
            name="run",
        )

    def _apply_update_random_seed(
        self, job: temporalio.bridge.proto.workflow_activation.UpdateRandomSeed
    ) -> None:
        self._random.seed(job.randomness_seed)

    def _make_workflow_input(
        self, init_job: temporalio.bridge.proto.workflow_activation.InitializeWorkflow
    ) -> ExecuteWorkflowInput:
        # Set arg types, using raw values for dynamic
        arg_types = self._defn.arg_types
        if not self._defn.name:
            # Dynamic is just the raw value for each input value
            arg_types = [temporalio.common.RawValue] * len(init_job.arguments)
        args = self._convert_payloads(init_job.arguments, arg_types)
        # Put args in a list if dynamic
        if not self._defn.name:
            args = [args]

        return ExecuteWorkflowInput(
            type=self._defn.cls,
            # TODO(cretz): Remove cast when https://github.com/python/mypy/issues/5485 fixed
            run_fn=cast(Callable[..., Awaitable[Any]], self._defn.run_fn),
            args=args,
            headers=init_job.headers,
        )

    #### _Runtime direct workflow call overrides ####
    # These are in alphabetical order and all start with "workflow_".

    def workflow_all_handlers_finished(self) -> bool:
        return not self._in_progress_updates and not self._in_progress_signals

    def workflow_continue_as_new(
        self,
        *args: Any,
        workflow: Union[None, Callable, str],
        task_queue: Optional[str],
        run_timeout: Optional[timedelta],
        task_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        memo: Optional[Mapping[str, Any]],
        search_attributes: Optional[
            Union[
                temporalio.common.SearchAttributes,
                temporalio.common.TypedSearchAttributes,
            ]
        ],
        versioning_intent: Optional[temporalio.workflow.VersioningIntent],
    ) -> NoReturn:
        self._assert_not_read_only("continue as new")
        # Use definition if callable
        name: Optional[str] = None
        arg_types: Optional[List[Type]] = None
        if isinstance(workflow, str):
            name = workflow
        elif callable(workflow):
            defn = temporalio.workflow._Definition.must_from_run_fn(workflow)
            name = defn.name
            arg_types = defn.arg_types
        elif workflow is not None:
            raise TypeError("Workflow must be None, a string, or callable")

        self._outbound.continue_as_new(
            ContinueAsNewInput(
                workflow=name,
                args=args,
                task_queue=task_queue,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                retry_policy=retry_policy,
                memo=memo,
                search_attributes=search_attributes,
                headers={},
                arg_types=arg_types,
                versioning_intent=versioning_intent,
            )
        )
        # TODO(cretz): Why can't MyPy infer the above never returns?
        raise RuntimeError("Unreachable")

    def workflow_extern_functions(self) -> Mapping[str, Callable]:
        return self._extern_functions

    def workflow_get_current_build_id(self) -> str:
        if not self._deployment_version_for_current_task:
            return ""
        return self._deployment_version_for_current_task.build_id

    def workflow_get_current_deployment_version(
        self,
    ) -> Optional[temporalio.common.WorkerDeploymentVersion]:
        if not self._deployment_version_for_current_task:
            return None
        return temporalio.common.WorkerDeploymentVersion(
            build_id=self._deployment_version_for_current_task.build_id,
            deployment_name=self._deployment_version_for_current_task.deployment_name,
        )

    def workflow_get_current_history_length(self) -> int:
        return self._current_history_length

    def workflow_get_current_history_size(self) -> int:
        return self._current_history_size

    def workflow_get_external_workflow_handle(
        self, id: str, *, run_id: Optional[str]
    ) -> temporalio.workflow.ExternalWorkflowHandle[Any]:
        return _ExternalWorkflowHandle(self, id, run_id)

    def workflow_get_query_handler(self, name: Optional[str]) -> Optional[Callable]:
        defn = self._queries.get(name)
        if not defn:
            return None
        # Bind if a method
        return defn.bind_fn(self._object) if defn.is_method else defn.fn

    def workflow_get_signal_handler(self, name: Optional[str]) -> Optional[Callable]:
        defn = self._signals.get(name)
        if not defn:
            return None
        # Bind if a method
        return defn.bind_fn(self._object) if defn.is_method else defn.fn

    def workflow_get_update_handler(self, name: Optional[str]) -> Optional[Callable]:
        defn = self._updates.get(name)
        if not defn:
            return None
        # Bind if a method
        return defn.bind_fn(self._object) if defn.is_method else defn.fn

    def workflow_get_update_validator(self, name: Optional[str]) -> Optional[Callable]:
        defn = self._updates.get(name) or self._updates.get(None)
        if not defn or not defn.validator:
            return None
        # Bind if a method
        return defn.bind_validator(self._object) if defn.is_method else defn.validator

    def workflow_info(self) -> temporalio.workflow.Info:
        return self._outbound.info()

    def workflow_instance(self) -> Any:
        return self._object

    def workflow_is_continue_as_new_suggested(self) -> bool:
        return self._continue_as_new_suggested

    def workflow_is_replaying(self) -> bool:
        return self._is_replaying

    def workflow_memo(self) -> Mapping[str, Any]:
        if self._untyped_converted_memo is None:
            self._untyped_converted_memo = {
                k: self._payload_converter.from_payload(v)
                for k, v in self._info.raw_memo.items()
            }
        return self._untyped_converted_memo

    def workflow_memo_value(
        self, key: str, default: Any, *, type_hint: Optional[Type]
    ) -> Any:
        payload = self._info.raw_memo.get(key)
        if not payload:
            if default is temporalio.common._arg_unset:
                raise KeyError(f"Memo does not have a value for key {key}")
            return default
        return self._payload_converter.from_payload(
            payload,
            type_hint,  # type: ignore[arg-type]
        )

    def workflow_upsert_memo(self, updates: Mapping[str, Any]) -> None:
        # Converting before creating a command so that we don't leave a partial command in case of conversion failure.
        update_payloads = {}
        removals = []
        for k, v in updates.items():
            if v is None:
                # Intentionally not checking if memo exists, so that no-op removals show up in history too.
                removals.append(k)
            else:
                update_payloads[k] = self._payload_converter.to_payload(v)

        if not update_payloads and not removals:
            return

        command = self._add_command()
        fields = command.modify_workflow_properties.upserted_memo.fields

        # Updating memo inside info by downcasting to mutable mapping.
        mut_raw_memo = cast(
            MutableMapping[str, temporalio.api.common.v1.Payload],
            self._info.raw_memo,
        )

        for k, v in update_payloads.items():
            fields[k].CopyFrom(v)
            mut_raw_memo[k] = v

        if removals:
            null_payload = self._payload_converter.to_payload(None)
            for k in removals:
                fields[k].CopyFrom(null_payload)
                mut_raw_memo.pop(k, None)

        # Keeping deserialized memo dict in sync, if exists
        if self._untyped_converted_memo is not None:
            for k, v in update_payloads.items():
                self._untyped_converted_memo[k] = self._payload_converter.from_payload(
                    v
                )
            for k in removals:
                self._untyped_converted_memo.pop(k, None)

    def workflow_metric_meter(self) -> temporalio.common.MetricMeter:
        # Create if not present, which means using an extern function
        if not self._metric_meter:
            metric_meter = cast(_WorkflowExternFunctions, self._extern_functions)[
                "__temporal_get_metric_meter"
            ]()
            metric_meter = metric_meter.with_additional_attributes(
                {
                    "namespace": self._info.namespace,
                    "task_queue": self._info.task_queue,
                    "workflow_type": self._info.workflow_type,
                }
            )
            self._metric_meter = _ReplaySafeMetricMeter(metric_meter)
        return self._metric_meter

    def workflow_patch(self, id: str, *, deprecated: bool) -> bool:
        self._assert_not_read_only("patch")
        # We use a previous memoized result of this if present. If this is being
        # deprecated, we can still use memoized result and skip the command.
        use_patch = self._patches_memoized.get(id)
        if use_patch is not None:
            return use_patch

        use_patch = not self._is_replaying or id in self._patches_notified
        self._patches_memoized[id] = use_patch
        if use_patch:
            command = self._add_command()
            command.set_patch_marker.patch_id = id
            command.set_patch_marker.deprecated = deprecated
        return use_patch

    def workflow_payload_converter(self) -> temporalio.converter.PayloadConverter:
        return self._payload_converter

    def workflow_random(self) -> random.Random:
        self._assert_not_read_only("random")
        return self._random

    def workflow_set_query_handler(
        self, name: Optional[str], handler: Optional[Callable]
    ) -> None:
        self._assert_not_read_only("set query handler")
        if handler:
            if inspect.iscoroutinefunction(handler):
                warnings.warn(
                    "Queries as async def functions are deprecated",
                    DeprecationWarning,
                    stacklevel=3,
                )
            defn = temporalio.workflow._QueryDefinition(
                name=name, fn=handler, is_method=False
            )
            self._queries[name] = defn
            if defn.dynamic_vararg:
                warnings.warn(
                    "Dynamic queries with vararg third param is deprecated, use Sequence[RawValue]",
                    DeprecationWarning,
                    stacklevel=3,
                )
        else:
            self._queries.pop(name, None)

    def workflow_set_signal_handler(
        self, name: Optional[str], handler: Optional[Callable]
    ) -> None:
        self._assert_not_read_only("set signal handler")
        if handler:
            defn = temporalio.workflow._SignalDefinition(
                name=name, fn=handler, is_method=False
            )
            self._signals[name] = defn
            if defn.dynamic_vararg:
                warnings.warn(
                    "Dynamic signals with vararg third param is deprecated, use Sequence[RawValue]",
                    DeprecationWarning,
                    stacklevel=3,
                )
            # We have to send buffered signals to the handler if they apply
            if name:
                for job in self._buffered_signals.pop(name, []):
                    self._process_signal_job(defn, job)
            else:
                for jobs in self._buffered_signals.values():
                    for job in jobs:
                        self._process_signal_job(defn, job)
                self._buffered_signals.clear()
        else:
            self._signals.pop(name, None)

    def workflow_set_update_handler(
        self,
        name: Optional[str],
        handler: Optional[Callable],
        validator: Optional[Callable],
    ) -> None:
        self._assert_not_read_only("set update handler")
        if handler:
            defn = temporalio.workflow._UpdateDefinition(
                name=name, fn=handler, is_method=False
            )
            if validator is not None:
                defn.set_validator(validator)
            self._updates[name] = defn
            if defn.dynamic_vararg:
                raise RuntimeError(
                    "Dynamic updates do not support a vararg third param, use Sequence[RawValue]",
                )
        else:
            self._updates.pop(name, None)

    def workflow_start_activity(
        self,
        activity: Any,
        *args: Any,
        task_queue: Optional[str],
        result_type: Optional[Type],
        schedule_to_close_timeout: Optional[timedelta],
        schedule_to_start_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        heartbeat_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        cancellation_type: temporalio.workflow.ActivityCancellationType,
        activity_id: Optional[str],
        versioning_intent: Optional[temporalio.workflow.VersioningIntent],
        summary: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> temporalio.workflow.ActivityHandle[Any]:
        self._assert_not_read_only("start activity")
        # Get activity definition if it's callable
        name: str
        arg_types: Optional[List[Type]] = None
        ret_type = result_type
        if isinstance(activity, str):
            name = activity
        elif callable(activity):
            defn = temporalio.activity._Definition.must_from_callable(activity)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic activity explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Activity must be a string or callable")

        return self._outbound.start_activity(
            StartActivityInput(
                activity=name,
                args=args,
                activity_id=activity_id,
                task_queue=task_queue,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=retry_policy,
                cancellation_type=cancellation_type,
                headers={},
                disable_eager_execution=self._disable_eager_activity_execution,
                arg_types=arg_types,
                ret_type=ret_type,
                versioning_intent=versioning_intent,
                summary=summary,
                priority=priority,
            )
        )

    # workflow_start_child_workflow ret_type
    async def workflow_start_child_workflow(
        self,
        workflow: Any,
        *args: Any,
        id: str,
        task_queue: Optional[str],
        result_type: Optional[Type],
        cancellation_type: temporalio.workflow.ChildWorkflowCancellationType,
        parent_close_policy: temporalio.workflow.ParentClosePolicy,
        execution_timeout: Optional[timedelta],
        run_timeout: Optional[timedelta],
        task_timeout: Optional[timedelta],
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: Optional[temporalio.common.RetryPolicy],
        cron_schedule: str,
        memo: Optional[Mapping[str, Any]],
        search_attributes: Optional[
            Union[
                temporalio.common.SearchAttributes,
                temporalio.common.TypedSearchAttributes,
            ]
        ],
        versioning_intent: Optional[temporalio.workflow.VersioningIntent],
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> temporalio.workflow.ChildWorkflowHandle[Any, Any]:
        # Use definition if callable
        name: str
        arg_types: Optional[List[Type]] = None
        ret_type = result_type
        if isinstance(workflow, str):
            name = workflow
        elif callable(workflow):
            defn = temporalio.workflow._Definition.must_from_run_fn(workflow)  # pyright: ignore
            if not defn.name:
                raise TypeError("Cannot invoke dynamic workflow explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Workflow must be a string or callable")

        return await self._outbound.start_child_workflow(
            StartChildWorkflowInput(
                workflow=name,
                args=args,
                id=id,
                task_queue=task_queue,
                cancellation_type=cancellation_type,
                parent_close_policy=parent_close_policy,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                id_reuse_policy=id_reuse_policy,
                retry_policy=retry_policy,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
                headers={},
                arg_types=arg_types,
                ret_type=ret_type,
                versioning_intent=versioning_intent,
                static_summary=static_summary,
                static_details=static_details,
                priority=priority,
            )
        )

    def workflow_start_local_activity(
        self,
        activity: Any,
        *args: Any,
        result_type: Optional[Type],
        schedule_to_close_timeout: Optional[timedelta],
        schedule_to_start_timeout: Optional[timedelta],
        start_to_close_timeout: Optional[timedelta],
        retry_policy: Optional[temporalio.common.RetryPolicy],
        local_retry_threshold: Optional[timedelta],
        cancellation_type: temporalio.workflow.ActivityCancellationType,
        activity_id: Optional[str],
    ) -> temporalio.workflow.ActivityHandle[Any]:
        # Get activity definition if it's callable
        name: str
        arg_types: Optional[List[Type]] = None
        ret_type = result_type
        if isinstance(activity, str):
            name = activity
        elif callable(activity):
            defn = temporalio.activity._Definition.must_from_callable(activity)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic activity explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Activity must be a string or callable")

        cast(_WorkflowExternFunctions, self._extern_functions)[
            "__temporal_assert_local_activity_valid"
        ](name)

        return self._outbound.start_local_activity(
            StartLocalActivityInput(
                activity=name,
                args=args,
                activity_id=activity_id,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                retry_policy=retry_policy,
                local_retry_threshold=local_retry_threshold,
                cancellation_type=cancellation_type,
                headers={},
                arg_types=arg_types,
                ret_type=ret_type,
            )
        )

    async def workflow_start_nexus_operation(
        self,
        endpoint: str,
        service: str,
        operation: Union[nexusrpc.Operation[InputT, OutputT], str, Callable[..., Any]],
        input: Any,
        output_type: Optional[Type[OutputT]],
        schedule_to_close_timeout: Optional[timedelta],
        cancellation_type: temporalio.workflow.NexusOperationCancellationType,
        headers: Optional[Mapping[str, str]],
    ) -> temporalio.workflow.NexusOperationHandle[OutputT]:
        # start_nexus_operation
        return await self._outbound.start_nexus_operation(
            StartNexusOperationInput(
                endpoint=endpoint,
                service=service,
                operation=operation,
                input=input,
                output_type=output_type,
                schedule_to_close_timeout=schedule_to_close_timeout,
                cancellation_type=cancellation_type,
                headers=headers,
            )
        )

    def workflow_time_ns(self) -> int:
        return self._time_ns

    def workflow_upsert_search_attributes(
        self,
        attributes: Union[
            temporalio.common.SearchAttributes,
            Sequence[temporalio.common.SearchAttributeUpdate],
        ],
    ) -> None:
        v = self._add_command().upsert_workflow_search_attributes

        # Update the attrs on info, casting to their mutable forms first
        mut_attrs = cast(
            MutableMapping[str, temporalio.common.SearchAttributeValues],
            self._info.search_attributes,
        )
        mut_typed_attrs = cast(
            List[temporalio.common.SearchAttributePair],
            self._info.typed_search_attributes.search_attributes,
        )

        # Do different things for untyped (i.e. Mapping) and typed
        if isinstance(attributes, Mapping):
            # Update the keys in the existing dictionary. We keep exact values
            # sent in instead of any kind of normalization. This means empty
            # lists remain as empty lists which matches what the server does.
            mut_attrs.update(attributes)
            for k, vals in attributes.items():
                # Add to command
                v.search_attributes[k].CopyFrom(
                    temporalio.converter.encode_search_attribute_values(vals)
                )

                # To apply to typed search attributes we remove, replace, or add. We
                # don't know any of the key types, so we do our best.
                index = next(
                    (i for i, a in enumerate(mut_typed_attrs) if a.key.name == k), None
                )
                if not vals:
                    if index is not None:
                        del mut_typed_attrs[index]
                else:
                    # Attempt to derive type the value and ignore if we can't.
                    # No need to warn because we're already warning on using
                    # this deprecated form.
                    key = (
                        temporalio.common.SearchAttributeKey._guess_from_untyped_values(
                            k, vals
                        )
                    )
                    if key:
                        val: Any = vals
                        # If the key is not a keyword list, we only support
                        # single item lists
                        if (
                            key.indexed_value_type
                            != temporalio.common.SearchAttributeIndexedValueType.KEYWORD_LIST
                        ):
                            if len(vals) != 1:
                                continue
                            val = vals[0]
                        pair = temporalio.common.SearchAttributePair(key, val)
                        if index is None:
                            mut_typed_attrs.append(pair)
                        else:
                            mut_typed_attrs[index] = pair
        else:
            # Update typed and untyped keys, replacing typed as needed
            for update in attributes:
                # Set on command (delete is a proper null)
                v.search_attributes[update.key.name].CopyFrom(
                    temporalio.converter.encode_typed_search_attribute_value(
                        update.key, update.value
                    )
                )

                # Update typed and untyped in info
                index = next(
                    (
                        i
                        for i, a in enumerate(mut_typed_attrs)
                        if a.key.name == update.key.name
                    ),
                    None,
                )
                if update.value is None:
                    # Delete
                    if index is not None:
                        del mut_typed_attrs[index]
                    # Just empty-list the untyped one
                    mut_attrs[update.key.name] = cast(List[str], [])
                else:
                    # Update
                    pair = temporalio.common.SearchAttributePair(
                        update.key, update.value
                    )
                    if index is None:
                        mut_typed_attrs.append(pair)
                    else:
                        mut_typed_attrs[index] = pair
                    # Single-item list if not already a sequence for untyped
                    mut_attrs[update.key.name] = (
                        list(update.value)
                        if update.key.indexed_value_type
                        == temporalio.common.SearchAttributeIndexedValueType.KEYWORD_LIST
                        else [update.value]
                    )

    async def workflow_sleep(
        self, duration: float, *, summary: Optional[str] = None
    ) -> None:
        user_metadata = (
            temporalio.api.sdk.v1.UserMetadata(
                summary=self._payload_converter.to_payload(summary)
            )
            if summary
            else None
        )
        fut = self.create_future()
        self._timer_impl(
            duration,
            _TimerOptions(user_metadata=user_metadata),
            lambda: fut.set_result(None),
        )
        await fut

    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: Optional[float] = None,
        timeout_summary: Optional[str] = None,
    ) -> None:
        self._assert_not_read_only("wait condition")
        fut = self.create_future()
        self._conditions.append((fn, fut))
        user_metadata = (
            temporalio.api.sdk.v1.UserMetadata(
                summary=self._payload_converter.to_payload(timeout_summary)
            )
            if timeout_summary
            else None
        )
        ctxvars = contextvars.copy_context()

        async def in_context():
            _TimerOptionsCtxVar.set(_TimerOptions(user_metadata=user_metadata))
            await asyncio.wait_for(fut, timeout)

        await ctxvars.run(in_context)

    def workflow_get_current_details(self) -> str:
        return self._current_details

    def workflow_set_current_details(self, details: str):
        self._assert_not_read_only("set current details")
        self._current_details = details

    #### Calls from outbound impl ####
    # These are in alphabetical order and all start with "_outbound_".

    def _outbound_continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        # Just throw
        raise _ContinueAsNewError(self, input)

    def _outbound_schedule_activity(
        self,
        input: Union[StartActivityInput, StartLocalActivityInput],
    ) -> _ActivityHandle:
        # Validate
        if not input.start_to_close_timeout and not input.schedule_to_close_timeout:
            raise ValueError(
                "Activity must have start_to_close_timeout or schedule_to_close_timeout"
            )

        handle: _ActivityHandle

        # Function that runs in the handle
        async def run_activity() -> Any:
            while True:
                # Mark it as started each loop because backoff could cause it to
                # be marked as unstarted
                handle._started = True
                try:
                    # We have to shield because we don't want the underlying
                    # result future to be cancelled
                    return await asyncio.shield(handle._result_fut)
                except _ActivityDoBackoffError as err:
                    # We have to sleep then reschedule. Note this sleep can be
                    # cancelled like any other timer.
                    await asyncio.sleep(
                        err.backoff.backoff_duration.ToTimedelta().total_seconds()
                    )
                    handle._apply_schedule_command(err.backoff)
                    # We have to put the handle back on the pending activity
                    # dict with its new seq
                    self._pending_activities[handle._seq] = handle
                except asyncio.CancelledError:
                    # If an activity future completes at the same time as a cancellation is being processed, the cancellation would be swallowed
                    # _WorkflowLogicFlag.RAISE_ON_CANCELLING_COMPLETED_ACTIVITY will correctly reraise the exception
                    if handle._result_fut.done():
                        if (
                            not self._is_replaying
                            or _WorkflowLogicFlag.RAISE_ON_CANCELLING_COMPLETED_ACTIVITY
                            in self._current_internal_flags
                        ):
                            self._current_completion.successful.used_internal_flags.append(
                                _WorkflowLogicFlag.RAISE_ON_CANCELLING_COMPLETED_ACTIVITY
                            )
                            raise
                    # Send a cancel request to the activity
                    handle._apply_cancel_command(self._add_command())

        # Create the handle and set as pending
        handle = _ActivityHandle(self, input, run_activity())
        handle._apply_schedule_command()
        self._pending_activities[handle._seq] = handle
        return handle

    async def _outbound_signal_child_workflow(
        self, input: SignalChildWorkflowInput
    ) -> None:
        payloads = (
            self._payload_converter.to_payloads(input.args) if input.args else None
        )
        command = self._add_command()
        v = command.signal_external_workflow_execution
        v.child_workflow_id = input.child_workflow_id
        v.signal_name = input.signal
        if payloads:
            v.args.extend(payloads)
        if input.headers:
            temporalio.common._apply_headers(input.headers, v.headers)
        await self._signal_external_workflow(command)

    async def _outbound_signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        payloads = (
            self._payload_converter.to_payloads(input.args) if input.args else None
        )
        command = self._add_command()
        v = command.signal_external_workflow_execution
        v.workflow_execution.namespace = input.namespace
        v.workflow_execution.workflow_id = input.workflow_id
        if input.workflow_run_id:
            v.workflow_execution.run_id = input.workflow_run_id
        v.signal_name = input.signal
        if payloads:
            v.args.extend(payloads)
        if input.headers:
            temporalio.common._apply_headers(input.headers, v.headers)
        await self._signal_external_workflow(command)

    async def _outbound_start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> _ChildWorkflowHandle:
        handle: _ChildWorkflowHandle

        # Common code for handling cancel for start and run
        def apply_child_cancel_error() -> None:
            # Send a cancel request to the child
            cancel_command = self._add_command()
            handle._apply_cancel_command(cancel_command)
            # If the cancel command is for external workflow, we
            # have to add a seq and mark it pending
            if cancel_command.HasField("request_cancel_external_workflow_execution"):
                cancel_seq = self._next_seq("external_cancel")
                cancel_command.request_cancel_external_workflow_execution.seq = (
                    cancel_seq
                )
                # TODO(cretz): Nothing waits on this future, so how
                # if at all should we report child-workflow cancel
                # request failure?
                self._pending_external_cancels[cancel_seq] = self.create_future()

        # Function that runs in the handle
        async def run_child() -> Any:
            while True:
                try:
                    # We have to shield because we don't want the future itself
                    # to be cancelled
                    return await asyncio.shield(handle._result_fut)
                except asyncio.CancelledError:
                    apply_child_cancel_error()

        # Create the handle and set as pending
        handle = _ChildWorkflowHandle(
            self, self._next_seq("child_workflow"), input, run_child()
        )
        handle._apply_start_command()
        self._pending_child_workflows[handle._seq] = handle

        # Wait on start before returning
        while True:
            try:
                # We have to shield because we don't want the future itself
                # to be cancelled
                await asyncio.shield(handle._start_fut)
                return handle
            except asyncio.CancelledError:
                apply_child_cancel_error()

    async def _outbound_start_nexus_operation(
        self, input: StartNexusOperationInput[Any, OutputT]
    ) -> _NexusOperationHandle[OutputT]:
        # A Nexus operation handle contains two futures: self._start_fut is resolved as a result of
        # the Nexus operation starting (activation job: resolve_nexus_operation_start), and
        # self._result_fut is resolved as a result of the Nexus operation completing (activation
        # job: resolve_nexus_operation). The handle itself corresponds to an asyncio.Task which
        # waits on self.result_fut, handling CancelledError by emitting a
        # RequestCancelNexusOperation command. We do not return the handle until we receive
        # resolve_nexus_operation_start, like ChildWorkflowHandle and unlike ActivityHandle. Note
        # that a Nexus operation may complete synchronously (in which case both jobs will be sent in
        # the same activation, and start will be resolved without an operation token), or
        # asynchronously (in which case they may be sent in separate activations, and start will be
        # resolved with an operation token). See comments in tests/worker/test_nexus.py for worked
        # examples of the evolution of the resulting handle state machine in the sync and async
        # Nexus response cases.
        handle: _NexusOperationHandle[OutputT]

        async def operation_handle_fn() -> OutputT:
            while True:
                try:
                    return cast(OutputT, await asyncio.shield(handle._result_fut))
                except asyncio.CancelledError:
                    cancel_command = self._add_command()
                    handle._apply_cancel_command(cancel_command)

        handle = _NexusOperationHandle(
            self, self._next_seq("nexus_operation"), input, operation_handle_fn()
        )
        handle._apply_schedule_command()
        self._pending_nexus_operations[handle._seq] = handle

        while True:
            try:
                await asyncio.shield(handle._start_fut)
                return handle
            except asyncio.CancelledError:
                cancel_command = self._add_command()
                handle._apply_cancel_command(cancel_command)

    #### Miscellaneous helpers ####
    # These are in alphabetical order.

    def _add_command(self) -> temporalio.bridge.proto.workflow_commands.WorkflowCommand:
        self._assert_not_read_only("add command")
        return self._current_completion.successful.commands.add()

    @contextmanager
    def _as_read_only(self) -> Iterator[None]:
        prev_val = self._read_only
        self._read_only = True
        try:
            yield None
        finally:
            self._read_only = prev_val

    def _assert_not_read_only(
        self, action_attempted: str, *, allow_during_delete: bool = False
    ) -> None:
        if self._deleting and not allow_during_delete:
            raise _WorkflowBeingEvictedError(
                f"Ignoring {action_attempted} while evicting workflow. This is not an error."
            )
        if self._read_only:
            raise temporalio.workflow.ReadOnlyContextError(
                f"While in read-only function, action attempted: {action_attempted}"
            )

    async def _cancel_external_workflow(
        self,
        # Should not have seq set
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        seq = self._next_seq("external_cancel")
        done_fut = self.create_future()
        command.request_cancel_external_workflow_execution.seq = seq

        # Set as pending
        self._pending_external_cancels[seq] = done_fut

        # Wait until done (there is no cancelling a cancel request)
        await done_fut

    def _check_condition(self, fn: Callable[[], bool], fut: asyncio.Future) -> bool:
        if fn():
            fut.set_result(True)
            return True
        return False

    def _convert_payloads(
        self,
        payloads: Sequence[temporalio.api.common.v1.Payload],
        types: Optional[List[Type]],
    ) -> List[Any]:
        if not payloads:
            return []
        # Only use type hints if they match count
        if types and len(types) != len(payloads):
            types = None
        try:
            return self._payload_converter.from_payloads(
                payloads,
                type_hints=types,
            )
        except temporalio.exceptions.FailureError:
            # Don't wrap payload conversion errors that would fail the workflow
            raise
        except Exception as err:
            if self._is_workflow_failure_exception(err):
                raise
            raise RuntimeError("Failed decoding arguments") from err

    def _instantiate_workflow_object(self) -> Any:
        if not self._workflow_input:
            raise RuntimeError("Expected workflow input. This is a Python SDK bug.")

        if hasattr(self._defn.cls.__init__, "__temporal_workflow_init"):
            workflow_instance = self._defn.cls(*self._workflow_input.args)
        else:
            workflow_instance = self._defn.cls()

        if self._defn.versioning_behavior:
            self._versioning_behavior = self._defn.versioning_behavior
        # If there's a dynamic config function, call it now after we've instantiated the object
        # but before we start executing the workflow
        if self._defn.name is None and self._defn.dynamic_config_fn is not None:
            dynamic_config = None
            try:
                with self._as_read_only():
                    dynamic_config = self._defn.dynamic_config_fn(workflow_instance)
            except Exception as err:
                logger.exception(
                    f"Failed to run dynamic config function in workflow {self._info.workflow_type}"
                )
                # Treat as a task failure
                self._current_activation_error = err
                raise self._current_activation_error

            if dynamic_config:
                if dynamic_config.failure_exception_types is not None:
                    self._dynamic_failure_exception_types = (
                        dynamic_config.failure_exception_types
                    )
                if (
                    dynamic_config.versioning_behavior
                    != temporalio.common.VersioningBehavior.UNSPECIFIED
                ):
                    self._versioning_behavior = dynamic_config.versioning_behavior

        return workflow_instance

    def _is_workflow_failure_exception(self, err: BaseException) -> bool:
        # An exception is a failure instead of a task fail if it's already a
        # failure error or if it is a timeout error or if it is an instance of
        # any of the failure types in the worker or workflow-level setting
        wf_failure_exception_types = self._defn.failure_exception_types
        if self._dynamic_failure_exception_types is not None:
            wf_failure_exception_types = self._dynamic_failure_exception_types
        return (
            isinstance(err, temporalio.exceptions.FailureError)
            or isinstance(err, asyncio.TimeoutError)
            or any(isinstance(err, typ) for typ in wf_failure_exception_types)
            or any(
                isinstance(err, typ)
                for typ in self._worker_level_failure_exception_types
            )
        )

    def _warn_if_unfinished_handlers(self) -> None:
        def warnable(handler_executions: Iterable[HandlerExecution]):
            return [
                ex
                for ex in handler_executions
                if ex.unfinished_policy
                == temporalio.workflow.HandlerUnfinishedPolicy.WARN_AND_ABANDON
            ]

        warnable_updates = warnable(self._in_progress_updates.values())
        if warnable_updates:
            warnings.warn(
                temporalio.workflow.UnfinishedUpdateHandlersWarning(
                    _make_unfinished_update_handler_message(warnable_updates)
                )
            )

        warnable_signals = warnable(self._in_progress_signals.values())
        if warnable_signals:
            warnings.warn(
                temporalio.workflow.UnfinishedSignalHandlersWarning(
                    _make_unfinished_signal_handler_message(warnable_signals)
                )
            )

    def _next_seq(self, type: str) -> int:
        seq = self._curr_seqs.get(type, 0) + 1
        self._curr_seqs[type] = seq
        return seq

    def _process_handler_args(
        self,
        job_name: str,
        job_input: Sequence[temporalio.api.common.v1.Payload],
        defn_name: Optional[str],
        defn_arg_types: Optional[List[Type]],
        defn_dynamic_vararg: bool,
    ) -> List[Any]:
        # If dynamic old-style vararg, args become name + varargs of given arg
        # types. If dynamic new-style raw value sequence, args become name +
        # seq of raw values.
        if not defn_name and defn_dynamic_vararg:
            # Take off the string type hint for conversion
            arg_types = defn_arg_types[1:] if defn_arg_types else None
            return [job_name] + self._convert_payloads(job_input, arg_types)
        if not defn_name:
            return [
                job_name,
                self._convert_payloads(
                    job_input, [temporalio.common.RawValue] * len(job_input)
                ),
            ]
        return self._convert_payloads(job_input, defn_arg_types)

    def _process_signal_job(
        self,
        defn: temporalio.workflow._SignalDefinition,
        job: temporalio.bridge.proto.workflow_activation.SignalWorkflow,
    ) -> None:
        try:
            args = self._process_handler_args(
                job.signal_name,
                job.input,
                defn.name,
                defn.arg_types,
                defn.dynamic_vararg,
            )
        except Exception:
            logger.exception(
                f"Failed deserializing signal input for {job.signal_name}, dropping the signal"
            )
            return
        input = HandleSignalInput(
            signal=job.signal_name, args=args, headers=job.headers
        )

        self._handled_signals_seq += 1
        id = self._handled_signals_seq
        self._in_progress_signals[id] = HandlerExecution(
            job.signal_name, defn.unfinished_policy
        )

        def done_callback(f):
            self._in_progress_signals.pop(id, None)

        task = self.create_task(
            self._run_top_level_workflow_function(self._inbound.handle_signal(input)),
            name=f"signal: {job.signal_name}",
        )
        task.add_done_callback(done_callback)

    def _register_task(
        self,
        task: asyncio.Task,
        *,
        name: Optional[str],
    ) -> None:
        self._assert_not_read_only("create task")
        # Name not supported on older Python versions
        if sys.version_info >= (3, 8):
            # Put the workflow info at the end of the task name
            name = name or task.get_name()
            name += f" (workflow: {self._info.workflow_type}, id: {self._info.workflow_id}, run: {self._info.run_id})"
            task.set_name(name)
        # Add to and remove from our own non-weak set instead of relying on
        # Python's weak set which can collect these too early
        self._tasks.add(task)
        task.add_done_callback(self._tasks.remove)
        # When the workflow is GC'd (for whatever reason, e.g. eviction or
        # worker shutdown), still-open tasks get a message logged to the
        # exception handler about still pending. We disable this if the
        # attribute is available.
        if hasattr(task, "_log_destroy_pending"):
            setattr(task, "_log_destroy_pending", False)

    def _run_once(self, *, check_conditions: bool) -> None:
        try:
            asyncio._set_running_loop(self)

            # We instantiate the workflow class _inside_ here because __init__
            # needs to run with this event loop set. If we're deleting and
            # we've never initialized, we don't need to bother.
            if not self._object and not self._deleting:
                self._object = self._instantiate_workflow_object()

            # Run while there is anything ready
            while self._ready:
                # Run and remove all ready ones
                while self._ready:
                    handle = self._ready.popleft()
                    handle._run()

                    # Must throw here if not deleting. Only really set inside
                    # _run_top_level_workflow_function.
                    if self._current_activation_error and not self._deleting:
                        raise self._current_activation_error

                # Check conditions which may add to the ready list. Also remove
                # conditions whose futures have already cancelled (e.g. when
                # timed out).
                if check_conditions:
                    self._conditions[:] = [
                        t
                        for t in self._conditions
                        if not t[1].done() and not self._check_condition(*t)
                    ]
        finally:
            asyncio._set_running_loop(None)

    # This is used for the primary workflow function and signal handlers in
    # order to apply common exception handling to each
    async def _run_top_level_workflow_function(self, coro: Awaitable[None]) -> None:
        try:
            await coro
        except _ContinueAsNewError as err:
            logger.debug("Workflow requested continue as new")
            err._apply_command()
        except (Exception, asyncio.CancelledError) as err:
            # During tear down we can ignore exceptions. Technically the
            # command-adding done later would throw a not-in-workflow exception
            # we'd ignore later, but it's better to preempt it
            if self._deleting:
                if LOG_IGNORE_DURING_DELETE:
                    logger.debug(
                        "Ignoring exception while deleting workflow", exc_info=True
                    )
                return

            # Handle continue as new
            if isinstance(err, _ContinueAsNewError):
                logger.debug("Workflow requested continue as new")
                err._apply_command()
                return

            logger.debug(
                f"Workflow raised failure with run ID {self._info.run_id}",
                exc_info=True,
            )

            # All asyncio cancelled errors become Temporal cancelled errors
            if isinstance(err, asyncio.CancelledError):
                err = temporalio.exceptions.CancelledError(str(err))

            # If a cancel was ever requested and this is a cancellation, or an
            # activity/child cancellation, we add a cancel command. Technically
            # this means that a swallowed cancel followed by, say, an activity
            # cancel later on will show the workflow as cancelled. But this is
            # a Temporal limitation in that cancellation is a state not an
            # event.
            if self._cancel_requested and temporalio.exceptions.is_cancelled_exception(
                err
            ):
                self._add_command().cancel_workflow_execution.SetInParent()
            elif self._is_workflow_failure_exception(err):
                # All other failure errors fail the workflow
                self._set_workflow_failure(err)
            else:
                # All other exceptions fail the task
                self._current_activation_error = err

    def _set_workflow_failure(self, err: BaseException) -> None:
        # All other failure errors fail the workflow
        failure = self._add_command().fail_workflow_execution.failure
        failure.SetInParent()
        try:
            self._failure_converter.to_failure(err, self._payload_converter, failure)
        except Exception as inner_err:
            raise ValueError("Failed converting workflow exception") from inner_err

    async def _signal_external_workflow(
        self,
        # Should not have seq set
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        seq = self._next_seq("external_signal")
        done_fut = self.create_future()
        command.signal_external_workflow_execution.seq = seq

        # Set as pending
        self._pending_external_signals[seq] = done_fut

        # Wait until completed or cancelled
        while True:
            try:
                # We have to shield because we don't want the future itself
                # to be cancelled
                return await asyncio.shield(done_fut)
            except asyncio.CancelledError:
                cancel_command = self._add_command()
                cancel_command.cancel_signal_workflow.seq = seq

    def _stack_trace(self) -> str:
        stacks = []
        for task in list(self._tasks):
            # TODO(cretz): These stacks are not very clean currently
            frames = []
            for frame in task.get_stack():
                frames.append(
                    traceback.FrameSummary(
                        frame.f_code.co_filename, frame.f_lineno, frame.f_code.co_name
                    )
                )
            stacks.append(
                f"Stack for {task!r} (most recent call last):\n"
                + "\n".join(traceback.format_list(frames))
            )
        return "\n\n".join(stacks)

    def _enhanced_stack_trace(self) -> temporalio.api.sdk.v1.EnhancedStackTrace:
        sdk = temporalio.api.sdk.v1.StackTraceSDKInfo(
            name="sdk-python", version=__version__
        )

        # this is to use `open`
        with temporalio.workflow.unsafe.sandbox_unrestricted():
            sources: Dict[str, temporalio.api.sdk.v1.StackTraceFileSlice] = dict()
            stacks: List[temporalio.api.sdk.v1.StackTrace] = []

            # future TODO
            # site package filter list -- we want to filter out traces from Python's internals and our sdk's internals. This is what `internal_code` is for, but right now it's just set to false.

            for task in list(self._tasks):
                locations: List[temporalio.api.sdk.v1.StackTraceFileLocation] = []

                for frame in task.get_stack():
                    filename = frame.f_code.co_filename
                    line_number = frame.f_lineno
                    func_name = frame.f_code.co_name

                    if filename not in sources.keys():
                        try:
                            with open(filename, "r") as f:
                                code = f.read()
                        except OSError as ose:
                            code = f"Cannot access code.\n---\n{ose.strerror}"
                            # TODO possibly include sentinel/property for success of src scrape? work out with ui
                        except Exception:
                            code = f"Generic Error.\n\n{traceback.format_exc()}"

                        file_slice = temporalio.api.sdk.v1.StackTraceFileSlice(
                            line_offset=0, content=code
                        )

                        sources[filename] = file_slice

                    file_location = temporalio.api.sdk.v1.StackTraceFileLocation(
                        file_path=filename,
                        line=line_number,
                        column=-1,
                        function_name=func_name,
                        internal_code=False,
                    )

                    locations.append(file_location)

                stacks.append(temporalio.api.sdk.v1.StackTrace(locations=locations))

            est = temporalio.api.sdk.v1.EnhancedStackTrace(
                sdk=sdk, sources=sources, stacks=stacks
            )
            return est

    def _temporal_workflow_metadata(self) -> temporalio.api.sdk.v1.WorkflowMetadata:
        query_definitions = [
            temporalio.api.sdk.v1.WorkflowInteractionDefinition(
                name=qd.name or "",
                description=qd.description or "",
            )
            for qd in self._queries.values()
        ]
        query_definitions.sort(key=lambda qd: qd.name)
        signal_definitions = [
            temporalio.api.sdk.v1.WorkflowInteractionDefinition(
                name=sd.name or "",
                description=sd.description or "",
            )
            for sd in self._signals.values()
        ]
        signal_definitions.sort(key=lambda sd: sd.name)
        update_definitions = [
            temporalio.api.sdk.v1.WorkflowInteractionDefinition(
                name=ud.name or "",
                description=ud.description or "",
            )
            for ud in self._updates.values()
        ]
        update_definitions.sort(key=lambda ud: ud.name)
        wf_def = temporalio.api.sdk.v1.WorkflowDefinition(
            type=self._info.workflow_type,
            query_definitions=query_definitions,
            signal_definitions=signal_definitions,
            update_definitions=update_definitions,
        )
        cur_details = self.workflow_get_current_details()
        return temporalio.api.sdk.v1.WorkflowMetadata(
            definition=wf_def, current_details=cur_details
        )

    def _timer_impl(
        self,
        delay: float,
        options: _TimerOptions,
        callback: Callable[..., Any],
        *args: Any,
        context: Optional[contextvars.Context] = None,
    ):
        self._assert_not_read_only("schedule timer")
        # Delay must be positive
        if delay < 0:
            raise RuntimeError("Attempting to schedule timer with negative delay")

        # Create, schedule, and return
        seq = self._next_seq("timer")
        handle = _TimerHandle(
            seq, self.time() + delay, options, callback, args, self, context
        )
        handle._apply_start_command(self._add_command(), delay)
        self._pending_timers[seq] = handle
        return handle

    #### asyncio.AbstractEventLoop function impls ####
    # These are in the order defined in CPython's impl of the base class. Many
    # functions are intentionally not implemented/supported.

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        if not isinstance(handle, _TimerHandle):
            raise TypeError("Expected Temporal timer handle")
        if not self._pending_timers.pop(handle._seq, None):
            return
        handle._apply_cancel_command(self._add_command())

    def call_soon(
        self,
        callback: Callable[..., Any],
        *args: Any,
        context: Optional[contextvars.Context] = None,
    ) -> asyncio.Handle:
        # We need to allow this during delete because this is how tasks schedule
        # entire cancellation calls
        self._assert_not_read_only("schedule task", allow_during_delete=True)
        handle = asyncio.Handle(callback, args, self, context)
        self._ready.append(handle)
        return handle

    def call_later(
        self,
        delay: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Optional[contextvars.Context] = None,
    ) -> asyncio.TimerHandle:
        options = _TimerOptionsCtxVar.get()
        return self._timer_impl(delay, options, callback, *args, context=context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., Any],
        *args: Any,
        context: Optional[contextvars.Context] = None,
    ) -> asyncio.TimerHandle:
        # We usually would not support fixed-future-time call (and we didn't
        # previously), but 3.11 added asyncio.timeout which uses it and 3.12
        # changed wait_for to use asyncio.timeout. We now simply count on users
        # to only add to loop.time() and not an actual fixed point. Due to the
        # fact that loop.time() is at a nanosecond level which floats can't
        # always express well, we round to the nearest millisecond. We do this
        # after the subtraction not before because this may be the result of a
        # previous addition in Python code.
        return self.call_later(
            round(when - self.time(), 3), callback, *args, context=context
        )

    def time(self) -> float:
        return self._time_ns / 1e9

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task(
        self,
        coro: Union[Awaitable[_T], Generator[Any, None, _T]],
        *,
        name: Optional[str] = None,
        context: Optional[contextvars.Context] = None,
    ) -> asyncio.Task[_T]:
        # Context only supported on newer Python versions
        if sys.version_info >= (3, 11):
            task = asyncio.Task(coro, loop=self, context=context)  # type: ignore
        else:
            task = asyncio.Task(coro, loop=self)  # type: ignore
        self._register_task(task, name=name)
        return task

    def get_exception_handler(self) -> Optional[_ExceptionHandler]:
        return self._exception_handler

    def get_task_factory(self) -> None:
        return None

    def set_exception_handler(self, handler: Optional[_ExceptionHandler]) -> None:
        self._exception_handler = handler

    def default_exception_handler(self, context: _Context) -> None:
        # Copied and slightly modified from
        # asyncio.BaseEventLoop.default_exception_handler
        message = context.get("message")
        if not message:
            message = "Unhandled exception in event loop"

        exception = context.get("exception")
        exc_info: Any
        if exception is not None:
            exc_info = (type(exception), exception, exception.__traceback__)
        else:
            exc_info = False

        log_lines = [message]
        for key in sorted(context):
            if key in {"message", "exception"}:
                continue
            value = context[key]
            if key == "source_traceback":
                tb = "".join(traceback.format_list(value))
                value = "Object created at (most recent call last):\n"
                value += tb.rstrip()
            elif key == "handle_traceback":
                tb = "".join(traceback.format_list(value))
                value = "Handle created at (most recent call last):\n"
                value += tb.rstrip()
            else:
                value = repr(value)
            log_lines.append(f"{key}: {value}")

        logger.error("\n".join(log_lines), exc_info=exc_info)

    def call_exception_handler(self, context: _Context) -> None:
        # Do nothing with any uncaught exceptions while deleting
        if self._deleting:
            return
        # Copied and slightly modified from
        # asyncio.BaseEventLoop.call_exception_handler
        if self._exception_handler is None:
            try:
                self.default_exception_handler(context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException:
                # Second protection layer for unexpected errors
                # in the default implementation, as well as for subclassed
                # event loops with overloaded "default_exception_handler".
                logger.error("Exception in default exception handler", exc_info=True)
        else:
            try:
                self._exception_handler(self, context)
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                # Exception in the user set custom exception handler.
                try:
                    # Let's try default handler.
                    self.default_exception_handler(
                        {
                            "message": "Unhandled error in exception handler",
                            "exception": exc,
                            "context": context,
                        }
                    )
                except (SystemExit, KeyboardInterrupt):
                    raise
                except BaseException:
                    # Guard 'default_exception_handler' in case it is
                    # overloaded.
                    logger.error(
                        "Exception in default exception handler "
                        "while handling an unexpected error "
                        "in custom exception handler",
                        exc_info=True,
                    )

    def get_debug(self) -> bool:
        return False


class _WorkflowInboundImpl(WorkflowInboundInterceptor):
    def __init__(  # type: ignore
        self,
        instance: _WorkflowInstanceImpl,
    ) -> None:
        # We are intentionally not calling the base class's __init__ here
        self._instance = instance

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        self._outbound = outbound  # type: ignore

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        args = [self._instance._object] + list(input.args)
        return await input.run_fn(*args)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        handler = self._instance.workflow_get_signal_handler(
            input.signal
        ) or self._instance.workflow_get_signal_handler(None)
        # Handler should always be present at this point
        assert handler
        if inspect.iscoroutinefunction(handler):
            await handler(*input.args)
        else:
            handler(*input.args)

    async def handle_query(self, input: HandleQueryInput) -> Any:
        handler = self._instance.workflow_get_query_handler(
            input.query
        ) or self._instance.workflow_get_query_handler(None)
        # Handler should always be present at this point
        assert handler
        if inspect.iscoroutinefunction(handler):
            return await handler(*input.args)
        else:
            return handler(*input.args)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        # Do not "or None" the validator, since we only want to use the validator for
        # the specific named update - we shouldn't fall back to the dynamic validator
        # for some defined, named update which doesn't have a defined validator.
        handler = self._instance.workflow_get_update_validator(input.update)
        # Validator may not be defined
        if handler is not None:
            handler(*input.args)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        handler = self._instance.workflow_get_update_handler(
            input.update
        ) or self._instance.workflow_get_update_handler(None)
        # Handler should always be present at this point
        assert handler
        if inspect.iscoroutinefunction(handler):
            return await handler(*input.args)
        else:
            return handler(*input.args)


class _WorkflowOutboundImpl(WorkflowOutboundInterceptor):
    def __init__(self, instance: _WorkflowInstanceImpl) -> None:  # type: ignore
        # We are intentionally not calling the base class's __init__ here
        self._instance = instance

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        self._instance._outbound_continue_as_new(input)

    def info(self) -> temporalio.workflow.Info:
        return self._instance._info

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        return await self._instance._outbound_signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        await self._instance._outbound_signal_external_workflow(input)

    def start_activity(
        self, input: StartActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        return self._instance._outbound_schedule_activity(input)

    async def start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> temporalio.workflow.ChildWorkflowHandle[Any, Any]:
        return await self._instance._outbound_start_child_workflow(input)

    async def start_nexus_operation(
        self, input: StartNexusOperationInput[Any, OutputT]
    ) -> _NexusOperationHandle[OutputT]:
        return await self._instance._outbound_start_nexus_operation(input)

    def start_local_activity(
        self, input: StartLocalActivityInput
    ) -> temporalio.workflow.ActivityHandle[Any]:
        return self._instance._outbound_schedule_activity(input)


@dataclass(frozen=True)
class _TimerOptions:
    user_metadata: Optional[temporalio.api.sdk.v1.UserMetadata] = None


_TimerOptionsCtxVar: contextvars.ContextVar[_TimerOptions] = contextvars.ContextVar(
    "__temporal_timer_options", default=_TimerOptions()
)


class _TimerHandle(asyncio.TimerHandle):
    def __init__(
        self,
        seq: int,
        when: float,
        options: Optional[_TimerOptions],
        callback: Callable[..., Any],
        args: Sequence[Any],
        loop: asyncio.AbstractEventLoop,
        context: Optional[contextvars.Context],
    ) -> None:
        super().__init__(when, callback, args, loop, context)
        self._seq = seq
        self._options = options

    def _apply_start_command(
        self,
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
        delay: float,
    ) -> None:
        command.start_timer.seq = self._seq
        if self._options and self._options.user_metadata:
            command.user_metadata.CopyFrom(self._options.user_metadata)
        command.start_timer.start_to_fire_timeout.FromNanoseconds(int(delay * 1e9))

    def _apply_cancel_command(
        self,
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        command.cancel_timer.seq = self._seq


class _ActivityDoBackoffError(BaseException):
    def __init__(
        self, backoff: temporalio.bridge.proto.activity_result.DoBackoff
    ) -> None:
        super().__init__("Backoff")
        self.backoff = backoff


class _ActivityHandle(temporalio.workflow.ActivityHandle[Any]):
    def __init__(
        self,
        instance: _WorkflowInstanceImpl,
        input: Union[StartActivityInput, StartLocalActivityInput],
        fn: Coroutine[Any, Any, Any],
    ) -> None:
        super().__init__(fn)
        self._instance = instance
        self._seq = instance._next_seq("activity")
        self._input = input
        self._result_fut = instance.create_future()
        self._started = False
        instance._register_task(self, name=f"activity: {input.activity}")

    def cancel(self, msg: Optional[Any] = None) -> bool:
        # Allow the cancel to go through for the task even if we're deleting,
        # just don't do any commands
        if not self._instance._deleting:
            self._instance._assert_not_read_only("cancel activity handle")
            # We override this because if it's not yet started and not done, we need
            # to send a cancel command because the async function won't run to trap
            # the cancel (i.e. cancelled before started)
            if not self._started and not self.done():
                self._apply_cancel_command(self._instance._add_command())
        # Message not supported in older versions
        if sys.version_info < (3, 9):
            return super().cancel()
        return super().cancel(msg)

    def _resolve_success(self, result: Any) -> None:
        # We intentionally let this error if already done
        self._result_fut.set_result(result)

    def _resolve_failure(self, err: BaseException) -> None:
        # If it was never started, we don't need to set this failure. In cases
        # where this is cancelled before started, setting this exception causes
        # a Python warning to be emitted because this future is never awaited
        # on.
        if self._started:
            self._result_fut.set_exception(err)

    def _resolve_backoff(
        self, backoff: temporalio.bridge.proto.activity_result.DoBackoff
    ) -> None:
        # Change the sequence and set backoff exception on the current future
        self._seq = self._instance._next_seq("activity")
        self._result_fut.set_exception(_ActivityDoBackoffError(backoff))
        # Replace the result future since that one is resolved now
        self._result_fut = self._instance.create_future()
        # Mark this as not started since it's a new future
        self._started = False

    def _apply_schedule_command(
        self,
        local_backoff: Optional[
            temporalio.bridge.proto.activity_result.DoBackoff
        ] = None,
    ) -> None:
        # Convert arguments before creating command in case it raises error
        payloads = (
            self._instance._payload_converter.to_payloads(self._input.args)
            if self._input.args
            else None
        )

        command = self._instance._add_command()
        # TODO(cretz): Why can't MyPy infer this?
        v: Union[
            temporalio.bridge.proto.workflow_commands.ScheduleActivity,
            temporalio.bridge.proto.workflow_commands.ScheduleLocalActivity,
        ] = (
            command.schedule_local_activity
            if isinstance(self._input, StartLocalActivityInput)
            else command.schedule_activity
        )
        v.seq = self._seq
        v.activity_id = self._input.activity_id or str(self._seq)
        v.activity_type = self._input.activity
        if self._input.headers:
            temporalio.common._apply_headers(self._input.headers, v.headers)
        if payloads:
            v.arguments.extend(payloads)
        if self._input.schedule_to_close_timeout:
            v.schedule_to_close_timeout.FromTimedelta(
                self._input.schedule_to_close_timeout
            )
        if self._input.schedule_to_start_timeout:
            v.schedule_to_start_timeout.FromTimedelta(
                self._input.schedule_to_start_timeout
            )
        if self._input.start_to_close_timeout:
            v.start_to_close_timeout.FromTimedelta(self._input.start_to_close_timeout)
        if self._input.retry_policy:
            self._input.retry_policy.apply_to_proto(v.retry_policy)
        v.cancellation_type = cast(
            temporalio.bridge.proto.workflow_commands.ActivityCancellationType.ValueType,
            int(self._input.cancellation_type),
        )

        # Things specific to local or remote
        if isinstance(self._input, StartActivityInput):
            command.schedule_activity.task_queue = (
                self._input.task_queue or self._instance._info.task_queue
            )
            if self._input.heartbeat_timeout:
                command.schedule_activity.heartbeat_timeout.FromTimedelta(
                    self._input.heartbeat_timeout
                )
            command.schedule_activity.do_not_eagerly_execute = (
                self._input.disable_eager_execution
            )
            if self._input.versioning_intent:
                command.schedule_activity.versioning_intent = (
                    self._input.versioning_intent._to_proto()
                )
            if self._input.summary:
                command.user_metadata.summary.CopyFrom(
                    self._instance._payload_converter.to_payload(self._input.summary)
                )
            if self._input.priority:
                command.schedule_activity.priority.CopyFrom(
                    self._input.priority._to_proto()
                )
        if isinstance(self._input, StartLocalActivityInput):
            if self._input.local_retry_threshold:
                command.schedule_local_activity.local_retry_threshold.FromTimedelta(
                    self._input.local_retry_threshold
                )
            if local_backoff:
                command.schedule_local_activity.attempt = local_backoff.attempt
                command.schedule_local_activity.original_schedule_time.CopyFrom(
                    local_backoff.original_schedule_time
                )

    def _apply_cancel_command(
        self,
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        if isinstance(self._input, StartActivityInput):
            command.request_cancel_activity.seq = self._seq
        else:
            command.request_cancel_local_activity.seq = self._seq


class _ChildWorkflowHandle(temporalio.workflow.ChildWorkflowHandle[Any, Any]):
    def __init__(
        self,
        instance: _WorkflowInstanceImpl,
        seq: int,
        input: StartChildWorkflowInput,
        fn: Coroutine[Any, Any, Any],
    ) -> None:
        super().__init__(fn)
        self._instance = instance
        self._seq = seq
        self._input = input
        self._start_fut: asyncio.Future[None] = instance.create_future()
        self._result_fut: asyncio.Future[Any] = instance.create_future()
        self._first_execution_run_id = "<unknown>"
        instance._register_task(self, name=f"child: {input.workflow}")

    @property
    def id(self) -> str:
        return self._input.id

    @property
    def first_execution_run_id(self) -> Optional[str]:
        return self._first_execution_run_id

    async def signal(
        self,
        signal: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        self._instance._assert_not_read_only("signal child handle")
        await self._instance._outbound.signal_child_workflow(
            SignalChildWorkflowInput(
                signal=temporalio.workflow._SignalDefinition.must_name_from_fn_or_str(
                    signal
                ),
                args=temporalio.common._arg_or_args(arg, args),
                child_workflow_id=self._input.id,
                headers={},
            )
        )

    def _resolve_start_success(self, run_id: str) -> None:
        self._first_execution_run_id = run_id
        # We intentionally let this error if already done
        self._start_fut.set_result(None)

    def _resolve_success(self, result: Any) -> None:
        # We intentionally let this error if already done
        self._result_fut.set_result(result)

    def _resolve_failure(self, err: BaseException) -> None:
        if self._start_fut.done():
            # We intentionally let this error if already done
            self._result_fut.set_exception(err)
        else:
            self._start_fut.set_exception(err)
            # Set the result as none to avoid Python warning about unhandled
            # future
            self._result_fut.set_result(None)

    def _apply_start_command(self) -> None:
        # Convert arguments before creating command in case it raises error
        payloads = (
            self._instance._payload_converter.to_payloads(self._input.args)
            if self._input.args
            else None
        )

        command = self._instance._add_command()
        v = command.start_child_workflow_execution
        v.seq = self._seq
        v.namespace = self._instance._info.namespace
        v.workflow_id = self._input.id
        v.workflow_type = self._input.workflow
        v.task_queue = self._input.task_queue or self._instance._info.task_queue
        if payloads:
            v.input.extend(payloads)
        if self._input.execution_timeout:
            v.workflow_execution_timeout.FromTimedelta(self._input.execution_timeout)
        if self._input.run_timeout:
            v.workflow_run_timeout.FromTimedelta(self._input.run_timeout)
        if self._input.task_timeout:
            v.workflow_task_timeout.FromTimedelta(self._input.task_timeout)
        v.parent_close_policy = cast(
            temporalio.bridge.proto.child_workflow.ParentClosePolicy.ValueType,
            int(self._input.parent_close_policy),
        )
        v.workflow_id_reuse_policy = cast(
            "temporalio.api.enums.v1.WorkflowIdReusePolicy.ValueType",
            int(self._input.id_reuse_policy),
        )
        if self._input.retry_policy:
            self._input.retry_policy.apply_to_proto(v.retry_policy)
        v.cron_schedule = self._input.cron_schedule
        if self._input.headers:
            temporalio.common._apply_headers(self._input.headers, v.headers)
        if self._input.memo:
            for k, val in self._input.memo.items():
                v.memo[k].CopyFrom(
                    self._instance._payload_converter.to_payloads([val])[0]
                )
        if self._input.search_attributes:
            _encode_search_attributes(
                self._input.search_attributes, v.search_attributes
            )
        v.cancellation_type = cast(
            temporalio.bridge.proto.child_workflow.ChildWorkflowCancellationType.ValueType,
            int(self._input.cancellation_type),
        )
        if self._input.versioning_intent:
            v.versioning_intent = self._input.versioning_intent._to_proto()
        if self._input.static_summary:
            command.user_metadata.summary.CopyFrom(
                self._instance._payload_converter.to_payload(self._input.static_summary)
            )
        if self._input.static_details:
            command.user_metadata.details.CopyFrom(
                self._instance._payload_converter.to_payload(self._input.static_details)
            )
        if self._input.priority:
            v.priority.CopyFrom(self._input.priority._to_proto())

    # If request cancel external, result does _not_ have seq
    def _apply_cancel_command(
        self,
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        command.cancel_child_workflow_execution.child_workflow_seq = self._seq


class _ExternalWorkflowHandle(temporalio.workflow.ExternalWorkflowHandle[Any]):
    def __init__(
        self,
        instance: _WorkflowInstanceImpl,
        id: str,
        run_id: Optional[str],
    ) -> None:
        super().__init__()
        self._instance = instance
        self._id = id
        self._run_id = run_id

    @property
    def id(self) -> str:
        return self._id

    @property
    def run_id(self) -> Optional[str]:
        return self._run_id

    async def signal(
        self,
        signal: Union[str, Callable],
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        self._instance._assert_not_read_only("signal external handle")
        await self._instance._outbound.signal_external_workflow(
            SignalExternalWorkflowInput(
                signal=temporalio.workflow._SignalDefinition.must_name_from_fn_or_str(
                    signal
                ),
                args=temporalio.common._arg_or_args(arg, args),
                namespace=self._instance._info.namespace,
                workflow_id=self._id,
                workflow_run_id=self._run_id,
                headers={},
            )
        )

    async def cancel(self) -> None:
        self._instance._assert_not_read_only("cancel external handle")
        command = self._instance._add_command()
        v = command.request_cancel_external_workflow_execution
        v.workflow_execution.namespace = self._instance._info.namespace
        v.workflow_execution.workflow_id = self._id
        if self._run_id:
            v.workflow_execution.run_id = self._run_id
        await self._instance._cancel_external_workflow(command)


# TODO(nexus-preview): are we sure we don't want to inherit from asyncio.Task as
# ActivityHandle and ChildWorkflowHandle do? I worry that we should provide .done(),
# .result(), .exception() etc for consistency.
class _NexusOperationHandle(temporalio.workflow.NexusOperationHandle[OutputT]):
    def __init__(
        self,
        instance: _WorkflowInstanceImpl,
        seq: int,
        input: StartNexusOperationInput[Any, OutputT],
        fn: Coroutine[Any, Any, OutputT],
    ):
        self._instance = instance
        self._seq = seq
        self._input = input
        self._task = asyncio.Task(fn)
        self._start_fut: asyncio.Future[Optional[str]] = instance.create_future()
        self._result_fut: asyncio.Future[Optional[OutputT]] = instance.create_future()

    @property
    def operation_token(self) -> Optional[str]:
        try:
            return self._start_fut.result()
        except BaseException:
            return None

    def __await__(self) -> Generator[Any, Any, OutputT]:
        return self._task.__await__()

    def __repr__(self) -> str:
        return (
            f"{self._start_fut} "
            f"{self._result_fut} "
            f"Task[{self._task._state}] fut_waiter = {self._task._fut_waiter}) ({self._task._must_cancel})"  # type: ignore
        )

    def cancel(self) -> bool:
        return self._task.cancel()

    def _resolve_start_success(self, operation_token: Optional[str]) -> None:
        # We intentionally let this error if already done
        self._start_fut.set_result(operation_token)

    def _resolve_success(self, result: Any) -> None:
        # We intentionally let this error if already done
        self._result_fut.set_result(result)

    def _resolve_failure(self, err: BaseException) -> None:
        if self._start_fut.done():
            # We intentionally let this error if already done
            self._result_fut.set_exception(err)
        else:
            self._start_fut.set_exception(err)
            # Set null result to avoid warning about unhandled future
            self._result_fut.set_result(None)

    def _apply_schedule_command(self) -> None:
        payload = self._instance._payload_converter.to_payload(self._input.input)
        command = self._instance._add_command()
        v = command.schedule_nexus_operation
        v.seq = self._seq
        v.endpoint = self._input.endpoint
        v.service = self._input.service
        v.operation = self._input.operation_name
        v.input.CopyFrom(payload)
        if self._input.schedule_to_close_timeout is not None:
            v.schedule_to_close_timeout.FromTimedelta(
                self._input.schedule_to_close_timeout
            )
        v.cancellation_type = cast(
            temporalio.bridge.proto.nexus.NexusOperationCancellationType.ValueType,
            int(self._input.cancellation_type),
        )

        if self._input.headers:
            for key, val in self._input.headers.items():
                v.nexus_header[key] = val

    def _apply_cancel_command(
        self,
        command: temporalio.bridge.proto.workflow_commands.WorkflowCommand,
    ) -> None:
        command.request_cancel_nexus_operation.seq = self._seq


class _ContinueAsNewError(temporalio.workflow.ContinueAsNewError):
    def __init__(
        self, instance: _WorkflowInstanceImpl, input: ContinueAsNewInput
    ) -> None:
        super().__init__("Continue as new")
        self._instance = instance
        self._input = input

    def _apply_command(self) -> None:
        # Convert arguments before creating command in case it raises error
        payloads = (
            self._instance._payload_converter.to_payloads(self._input.args)
            if self._input.args
            else None
        )
        memo_payloads = (
            {
                k: self._instance._payload_converter.to_payloads([val])[0]
                for k, val in self._input.memo.items()
            }
            if self._input.memo
            else None
        )

        command = self._instance._add_command()
        v = command.continue_as_new_workflow_execution
        v.SetInParent()
        if self._input.workflow:
            v.workflow_type = self._input.workflow
        if self._input.task_queue:
            v.task_queue = self._input.task_queue
        if payloads:
            v.arguments.extend(payloads)
        if self._input.run_timeout:
            v.workflow_run_timeout.FromTimedelta(self._input.run_timeout)
        if self._input.task_timeout:
            v.workflow_task_timeout.FromTimedelta(self._input.task_timeout)
        if self._input.headers:
            temporalio.common._apply_headers(self._input.headers, v.headers)
        if self._input.retry_policy:
            self._input.retry_policy.apply_to_proto(v.retry_policy)
        if memo_payloads:
            for k, val in memo_payloads.items():
                v.memo[k].CopyFrom(val)
        if self._input.search_attributes:
            _encode_search_attributes(
                self._input.search_attributes, v.search_attributes
            )
        if self._input.versioning_intent:
            v.versioning_intent = self._input.versioning_intent._to_proto()


def _encode_search_attributes(
    attributes: Union[
        temporalio.common.SearchAttributes, temporalio.common.TypedSearchAttributes
    ],
    payloads: Mapping[str, temporalio.api.common.v1.Payload],
) -> None:
    if isinstance(attributes, temporalio.common.TypedSearchAttributes):
        for pair in attributes:
            payloads[pair.key.name].CopyFrom(
                temporalio.converter.encode_typed_search_attribute_value(
                    pair.key, pair.value
                )
            )
    else:
        for k, vals in attributes.items():
            payloads[k].CopyFrom(
                temporalio.converter.encode_search_attribute_values(vals)
            )


class _WorkflowExternFunctions(TypedDict):
    __temporal_get_metric_meter: Callable[[], temporalio.common.MetricMeter]
    __temporal_assert_local_activity_valid: Callable[[str], None]


class _ReplaySafeMetricMeter(temporalio.common.MetricMeter):
    def __init__(self, underlying: temporalio.common.MetricMeter) -> None:
        self._underlying = underlying

    def create_counter(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricCounter:
        return _ReplaySafeMetricCounter(
            self._underlying.create_counter(name, description, unit)
        )

    def create_histogram(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricHistogram:
        return _ReplaySafeMetricHistogram(
            self._underlying.create_histogram(name, description, unit)
        )

    def create_histogram_float(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricHistogramFloat:
        return _ReplaySafeMetricHistogramFloat(
            self._underlying.create_histogram_float(name, description, unit)
        )

    def create_histogram_timedelta(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricHistogramTimedelta:
        return _ReplaySafeMetricHistogramTimedelta(
            self._underlying.create_histogram_timedelta(name, description, unit)
        )

    def create_gauge(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricGauge:
        return _ReplaySafeMetricGauge(
            self._underlying.create_gauge(name, description, unit)
        )

    def create_gauge_float(
        self, name: str, description: Optional[str] = None, unit: Optional[str] = None
    ) -> temporalio.common.MetricGaugeFloat:
        return _ReplaySafeMetricGaugeFloat(
            self._underlying.create_gauge_float(name, description, unit)
        )

    def with_additional_attributes(
        self, additional_attributes: temporalio.common.MetricAttributes
    ) -> temporalio.common.MetricMeter:
        return _ReplaySafeMetricMeter(
            self._underlying.with_additional_attributes(additional_attributes)
        )


_MetricType = TypeVar("_MetricType", bound=temporalio.common.MetricCommon)


class _ReplaySafeMetricCommon(temporalio.common.MetricCommon, Generic[_MetricType]):
    def __init__(self, underlying: _MetricType) -> None:
        self._underlying = underlying

    @property
    def name(self) -> str:
        return self._underlying.name

    @property
    def description(self) -> Optional[str]:
        return self._underlying.description

    @property
    def unit(self) -> Optional[str]:
        return self._underlying.unit

    def with_additional_attributes(
        self, additional_attributes: temporalio.common.MetricAttributes
    ) -> Self:
        return self.__class__(
            self._underlying.with_additional_attributes(additional_attributes)
        )


class _ReplaySafeMetricCounter(
    temporalio.common.MetricCounter,
    _ReplaySafeMetricCommon[temporalio.common.MetricCounter],
):
    def add(
        self,
        value: int,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.add(value, additional_attributes)


class _ReplaySafeMetricHistogram(
    temporalio.common.MetricHistogram,
    _ReplaySafeMetricCommon[temporalio.common.MetricHistogram],
):
    def record(
        self,
        value: int,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.record(value, additional_attributes)


class _ReplaySafeMetricHistogramFloat(
    temporalio.common.MetricHistogramFloat,
    _ReplaySafeMetricCommon[temporalio.common.MetricHistogramFloat],
):
    def record(
        self,
        value: float,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.record(value, additional_attributes)


class _ReplaySafeMetricHistogramTimedelta(
    temporalio.common.MetricHistogramTimedelta,
    _ReplaySafeMetricCommon[temporalio.common.MetricHistogramTimedelta],
):
    def record(
        self,
        value: timedelta,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.record(value, additional_attributes)


class _ReplaySafeMetricGauge(
    temporalio.common.MetricGauge,
    _ReplaySafeMetricCommon[temporalio.common.MetricGauge],
):
    def set(
        self,
        value: int,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.set(value, additional_attributes)


class _ReplaySafeMetricGaugeFloat(
    temporalio.common.MetricGaugeFloat,
    _ReplaySafeMetricCommon[temporalio.common.MetricGaugeFloat],
):
    def set(
        self,
        value: float,
        additional_attributes: Optional[temporalio.common.MetricAttributes] = None,
    ) -> None:
        if not temporalio.workflow.unsafe.is_replaying():
            self._underlying.set(value, additional_attributes)


class _WorkflowBeingEvictedError(BaseException):
    pass


@dataclass
class HandlerExecution:
    """Information about an execution of a signal or update handler."""

    name: str
    unfinished_policy: temporalio.workflow.HandlerUnfinishedPolicy
    id: Optional[str] = None


def _make_unfinished_update_handler_message(
    handler_executions: List[HandlerExecution],
) -> str:
    message = """
[TMPRL1102] Workflow finished while update handlers are still running. This may have interrupted work that the
update handler was doing, and the client that sent the update will receive a 'workflow execution
already completed' RPCError instead of the update result. You can wait for all update and signal
handlers to complete by using `await workflow.wait_condition(lambda:
workflow.all_handlers_finished())`. Alternatively, if both you and the clients sending the update
are okay with interrupting running handlers when the workflow finishes, and causing clients to
receive errors, then you can disable this warning via the update handler decorator:
`@workflow.update(unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON)`.
""".replace("\n", " ").strip()
    return (
        f"{message} The following updates were unfinished (and warnings were not disabled for their handler): "
        + json.dumps([{"name": ex.name, "id": ex.id} for ex in handler_executions])
    )


def _make_unfinished_signal_handler_message(
    handler_executions: List[HandlerExecution],
) -> str:
    message = """
[TMPRL1102] Workflow finished while signal handlers are still running. This may have interrupted work that the
signal handler was doing. You can wait for all update and signal handlers to complete by using
`await workflow.wait_condition(lambda: workflow.all_handlers_finished())`. Alternatively, if both
you and the clients sending the signal are okay with interrupting running handlers when the workflow
finishes, then you can disable this warning via the signal handler decorator:
`@workflow.signal(unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON)`.
""".replace("\n", " ").strip()
    names = collections.Counter(ex.name for ex in handler_executions)
    return (
        f"{message} The following signals were unfinished (and warnings were not disabled for their handler): "
        + json.dumps(
            [{"name": name, "count": count} for name, count in names.most_common()]
        )
    )


class _WorkflowLogicFlag(IntEnum):
    """Flags that may be set on task/activation completion to differentiate new from old workflow behavior."""

    RAISE_ON_CANCELLING_COMPLETED_ACTIVITY = 1
