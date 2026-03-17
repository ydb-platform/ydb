"""Workflow worker."""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import os
import sys
import threading
from datetime import timezone
from types import TracebackType
from typing import (
    Awaitable,
    Callable,
    Dict,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Set,
    Type,
)

import temporalio.activity
import temporalio.api.common.v1
import temporalio.bridge.client
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.runtime
import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import temporalio.workflow

from ._interceptor import (
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)
from ._workflow_instance import (
    WorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowRunner,
    _WorkflowExternFunctions,
)

logger = logging.getLogger(__name__)

# Set to true to log all activations and completions
LOG_PROTOS = False


class _WorkflowWorker:
    def __init__(
        self,
        *,
        bridge_worker: Callable[[], temporalio.bridge.worker.Worker],
        namespace: str,
        task_queue: str,
        workflows: Sequence[Type],
        workflow_task_executor: Optional[concurrent.futures.ThreadPoolExecutor],
        max_concurrent_workflow_tasks: Optional[int],
        workflow_runner: WorkflowRunner,
        unsandboxed_workflow_runner: WorkflowRunner,
        data_converter: temporalio.converter.DataConverter,
        interceptors: Sequence[Interceptor],
        workflow_failure_exception_types: Sequence[Type[BaseException]],
        debug_mode: bool,
        disable_eager_activity_execution: bool,
        metric_meter: temporalio.common.MetricMeter,
        on_eviction_hook: Optional[
            Callable[
                [str, temporalio.bridge.proto.workflow_activation.RemoveFromCache], None
            ]
        ],
        disable_safe_eviction: bool,
        should_enforce_versioning_behavior: bool,
        assert_local_activity_valid: Callable[[str], None],
        encode_headers: bool,
    ) -> None:
        self._bridge_worker = bridge_worker
        self._namespace = namespace
        self._task_queue = task_queue
        self._workflow_task_executor = (
            workflow_task_executor
            or concurrent.futures.ThreadPoolExecutor(
                max_workers=max_concurrent_workflow_tasks or 500,
                thread_name_prefix="temporal_workflow_",
            )
        )
        self._workflow_task_executor_user_provided = workflow_task_executor is not None
        self._workflow_runner = workflow_runner
        self._unsandboxed_workflow_runner = unsandboxed_workflow_runner
        self._data_converter = data_converter
        # Build the interceptor classes and collect extern functions
        self._extern_functions: MutableMapping[str, Callable] = {}
        self._interceptor_classes: List[Type[WorkflowInboundInterceptor]] = []
        interceptor_class_input = WorkflowInterceptorClassInput(
            unsafe_extern_functions=self._extern_functions
        )
        for i in interceptors:
            interceptor_class = i.workflow_interceptor_class(interceptor_class_input)
            if interceptor_class:
                self._interceptor_classes.append(interceptor_class)
        self._extern_functions.update(
            **_WorkflowExternFunctions(  # type: ignore
                __temporal_get_metric_meter=lambda: metric_meter,
                __temporal_assert_local_activity_valid=assert_local_activity_valid,
            )
        )

        self._workflow_failure_exception_types = workflow_failure_exception_types
        self._running_workflows: Dict[str, _RunningWorkflow] = {}
        self._disable_eager_activity_execution = disable_eager_activity_execution
        self._on_eviction_hook = on_eviction_hook
        self._disable_safe_eviction = disable_safe_eviction
        self._encode_headers = encode_headers
        self._throw_after_activation: Optional[Exception] = None

        # If there's a debug mode or a truthy TEMPORAL_DEBUG env var, disable
        # deadlock detection, otherwise set to 2 seconds
        self._deadlock_timeout_seconds = (
            None if debug_mode or os.environ.get("TEMPORAL_DEBUG") else 2
        )

        # Keep track of workflows that could not be evicted
        self._could_not_evict_count = 0

        # Set the worker-level failure exception types into the runner
        workflow_runner.set_worker_level_failure_exception_types(
            workflow_failure_exception_types
        )

        # Validate and build workflow dict
        self._workflows: Dict[str, temporalio.workflow._Definition] = {}
        self._dynamic_workflow: Optional[temporalio.workflow._Definition] = None
        for workflow in workflows:
            defn = temporalio.workflow._Definition.must_from_class(workflow)
            # Confirm name unique
            if defn.name in self._workflows:
                raise ValueError(f"More than one workflow named {defn.name}")
            if should_enforce_versioning_behavior:
                if (
                    defn.versioning_behavior
                    in [
                        None,
                        temporalio.common.VersioningBehavior.UNSPECIFIED,
                    ]
                    and not defn.dynamic_config_fn
                ):
                    raise ValueError(
                        f"Workflow {defn.name} must specify a versioning behavior using "
                        "the `versioning_behavior` argument to `@workflow.defn` or by "
                        "defining a function decorated with `@workflow.dynamic_config`."
                    )

            # Prepare the workflow with the runner (this will error in the
            # sandbox if an import fails somehow)
            try:
                if defn.sandboxed:
                    workflow_runner.prepare_workflow(defn)
                else:
                    unsandboxed_workflow_runner.prepare_workflow(defn)
            except Exception as err:
                raise RuntimeError(f"Failed validating workflow {defn.name}") from err
            if defn.name:
                self._workflows[defn.name] = defn
            elif self._dynamic_workflow:
                raise TypeError("More than one dynamic workflow")
            else:
                self._dynamic_workflow = defn

    async def run(self) -> None:
        # Continually poll for workflow work
        task_tag = object()
        try:
            while True:
                act = await self._bridge_worker().poll_workflow_activation()

                # Schedule this as a task, but we don't need to track it or
                # await it. Rather we'll give it an attribute and wait for it
                # when done.
                task = asyncio.create_task(self._handle_activation(act))
                setattr(task, "__temporal_task_tag", task_tag)
        except temporalio.bridge.worker.PollShutdownError:
            pass
        except Exception as err:
            raise RuntimeError("Workflow worker failed") from err
        finally:
            # Collect all tasks and wait for them to complete
            our_tasks = [
                t
                for t in asyncio.all_tasks()
                if getattr(t, "__temporal_task_tag", None) is task_tag
            ]
            if our_tasks:
                await asyncio.wait(our_tasks)
            # Shutdown the thread pool executor if we created it
            if not self._workflow_task_executor_user_provided:
                self._workflow_task_executor.shutdown()

        if self._throw_after_activation:
            raise self._throw_after_activation

    def notify_shutdown(self) -> None:
        if self._could_not_evict_count:
            logger.warning(
                f"Shutting down workflow worker, but {self._could_not_evict_count} "
                + "workflow(s) could not be evicted previously, so the shutdown may hang"
            )

    # Only call this if run() raised an error
    async def drain_poll_queue(self) -> None:
        while True:
            try:
                # Just take all tasks and say we can't handle them
                act = await self._bridge_worker().poll_workflow_activation()
                completion = temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion(
                    run_id=act.run_id
                )
                completion.failed.failure.message = "Worker shutting down"
                await self._bridge_worker().complete_workflow_activation(completion)
            except temporalio.bridge.worker.PollShutdownError:
                return

    async def _handle_activation(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> None:
        global LOG_PROTOS

        # Extract a couple of jobs from the activation
        cache_remove_job = None
        init_job = None
        for job in act.jobs:
            if job.HasField("remove_from_cache"):
                cache_remove_job = job.remove_from_cache
            elif job.HasField("initialize_workflow"):
                init_job = job.initialize_workflow

        # If this is a cache removal, it is handled separately
        if cache_remove_job:
            # Should never happen
            if len(act.jobs) != 1:
                logger.warning("Unexpected job alongside cache remove job")
            await self._handle_cache_eviction(act, cache_remove_job)
            return

        # Build default success completion (e.g. remove-job-only activations)
        completion = (
            temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion()
        )
        completion.successful.SetInParent()
        try:
            # Decode the activation if there's a codec and not cache remove job
            if self._data_converter.payload_codec:
                await temporalio.bridge.worker.decode_activation(
                    act,
                    self._data_converter.payload_codec,
                    decode_headers=self._encode_headers,
                )

            if LOG_PROTOS:
                logger.debug("Received workflow activation:\n%s", act)

            # If the workflow is not running yet, create it
            workflow = self._running_workflows.get(act.run_id)
            if not workflow:
                # Must have a initialize job to create instance
                if not init_job:
                    raise RuntimeError(
                        "Missing initialize workflow, workflow could have unexpectedly been removed from cache"
                    )
                workflow = _RunningWorkflow(
                    self._create_workflow_instance(act, init_job)
                )
                self._running_workflows[act.run_id] = workflow
            elif init_job:
                # This should never happen
                logger.warning(
                    "Cache already exists for activation with initialize job"
                )

            # Run activation in separate thread so we can check if it's
            # deadlocked
            if workflow:
                activate_task = asyncio.get_running_loop().run_in_executor(
                    self._workflow_task_executor,
                    workflow.activate,
                    act,
                )

                # Run activation task with deadlock timeout
                try:
                    completion = await asyncio.wait_for(
                        activate_task, self._deadlock_timeout_seconds
                    )
                except asyncio.TimeoutError:
                    # Need to create the deadlock exception up here so it
                    # captures the trace now instead of later after we may have
                    # interrupted it
                    deadlock_exc = _DeadlockError.from_deadlocked_workflow(
                        workflow.instance, self._deadlock_timeout_seconds
                    )
                    # When we deadlock, we will raise an exception to fail
                    # the task. But before we do that, we want to try to
                    # interrupt the thread and put this activation task on
                    # the workflow so that the successive eviction can wait
                    # on it before trying to evict.
                    workflow.attempt_deadlock_interruption()
                    # Set the task and raise
                    workflow.deadlocked_activation_task = activate_task
                    raise deadlock_exc from None

        except Exception as err:
            if isinstance(err, _DeadlockError):
                err.swap_traceback()

            logger.exception(
                "Failed handling activation on workflow with run ID %s", act.run_id
            )

            # Set completion failure
            completion.failed.failure.SetInParent()
            try:
                self._data_converter.failure_converter.to_failure(
                    err,
                    self._data_converter.payload_converter,
                    completion.failed.failure,
                )
            except Exception as inner_err:
                logger.exception(
                    "Failed converting activation exception on workflow with run ID %s",
                    act.run_id,
                )
                completion.failed.failure.message = (
                    f"Failed converting activation exception: {inner_err}"
                )

        # Always set the run ID on the completion
        completion.run_id = act.run_id

        # Encode the completion if there's a codec and not cache remove job
        if self._data_converter.payload_codec:
            try:
                await temporalio.bridge.worker.encode_completion(
                    completion,
                    self._data_converter.payload_codec,
                    encode_headers=self._encode_headers,
                )
            except Exception as err:
                logger.exception(
                    "Failed encoding completion on workflow with run ID %s", act.run_id
                )
                completion.failed.Clear()
                completion.failed.failure.message = f"Failed encoding completion: {err}"

        # Send off completion
        if LOG_PROTOS:
            logger.debug("Sending workflow completion:\n%s", completion)
        try:
            await self._bridge_worker().complete_workflow_activation(completion)
        except Exception:
            # TODO(cretz): Per others, this is supposed to crash the worker
            logger.exception(
                "Failed completing activation on workflow with run ID %s", act.run_id
            )

    async def _handle_cache_eviction(
        self,
        act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
        job: temporalio.bridge.proto.workflow_activation.RemoveFromCache,
    ) -> None:
        logger.debug(
            "Evicting workflow with run ID %s, message: %s", act.run_id, job.message
        )

        # Find the workflow to process safe eviction unless safe eviction
        # disabled
        workflow = None
        if not self._disable_safe_eviction:
            workflow = self._running_workflows.get(act.run_id)

        # Safe eviction...
        if workflow:
            # We have to wait on the deadlocked task if it is set. This is
            # because eviction may be the result of a deadlocked workflow but
            # we cannot safely evict until that task is done with its thread. We
            # don't care what errors may have occurred. We intentionally wait
            # forever which means a deadlocked task cannot be evicted and give
            # its slot back.
            if workflow.deadlocked_activation_task:
                logger.debug(
                    "Waiting for deadlocked task to complete on run %s", act.run_id
                )
                try:
                    await workflow.deadlocked_activation_task
                except:
                    pass

            # Process the activation to evict. It is very important that
            # eviction complete successfully because this is the only way we can
            # confirm the event loop was torn down gracefully and therefore no
            # GC'ing of the tasks occurs (which can cause them to wake up in
            # different threads). We will wait deadlock timeout amount (2s if
            # enabled) before making it clear to users that eviction is being
            # swallowed. Any error or timeout of eviction causes us to retry
            # forever because something in users code is preventing eviction.
            seen_fail = False
            handle_eviction_task: Optional[asyncio.Future] = None
            while True:
                try:
                    # We only create the eviction task if we haven't already or
                    # it is done. This is because if it already is running and
                    # timed out, it's still running (and holding on to a
                    # thread). But if did complete running but failed with
                    # another error, we want to re-create the task.
                    if not handle_eviction_task or handle_eviction_task.done():
                        handle_eviction_task = (
                            asyncio.get_running_loop().run_in_executor(
                                self._workflow_task_executor,
                                workflow.activate,
                                act,
                            )
                        )
                    await asyncio.wait_for(
                        handle_eviction_task, self._deadlock_timeout_seconds
                    )
                    # Break if it succeeds
                    break
                except BaseException as err:
                    # Only want to log and mark as could not evict once
                    if not seen_fail:
                        seen_fail = True
                        self._could_not_evict_count += 1
                        # We give a different message for timeout vs other
                        # exception
                        if isinstance(err, asyncio.TimeoutError):
                            logger.error(
                                "Timed out running eviction job for run ID %s, continually "
                                + "retrying eviction. This is usually caused by inadvertently "
                                + "catching 'BaseException's like asyncio.CancelledError or "
                                + "_WorkflowBeingEvictedError and still continuing work. "
                                + "Since eviction could not be processed, this worker "
                                + "may not complete and the slot may remain forever used "
                                + "unless it eventually completes.",
                                act.run_id,
                            )
                        else:
                            logger.exception(
                                "Failed running eviction job for run ID %s, continually retrying "
                                + "eviction. Since eviction could not be processed, this worker "
                                + "may not complete and the slot may remain forever used "
                                + "unless it eventually completes.",
                                act.run_id,
                            )
                    # We want to wait a couple of seconds before trying to evict again
                    await asyncio.sleep(2)
            # Decrement the could-not-evict-count if it finally succeeded
            if seen_fail:
                self._could_not_evict_count -= 1

        # Remove from map and send completion
        if act.run_id in self._running_workflows:
            del self._running_workflows[act.run_id]
        try:
            await self._bridge_worker().complete_workflow_activation(
                temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion(
                    run_id=act.run_id,
                    successful=temporalio.bridge.proto.workflow_completion.Success(),
                )
            )
        except Exception:
            logger.exception(
                "Failed completing eviction activation on workflow with run ID %s",
                act.run_id,
            )

        # Run eviction hook if present
        if self._on_eviction_hook is not None:
            try:
                self._on_eviction_hook(act.run_id, job)
            except Exception as e:
                self._throw_after_activation = e
                logger.debug("Shutting down worker on eviction hook exception")
                self._bridge_worker().initiate_shutdown()

    def _create_workflow_instance(
        self,
        act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
        init: temporalio.bridge.proto.workflow_activation.InitializeWorkflow,
    ) -> WorkflowInstance:
        # Get the definition
        defn = self._workflows.get(init.workflow_type, self._dynamic_workflow)
        if not defn:
            workflow_names = ", ".join(sorted(self._workflows.keys()))
            raise temporalio.exceptions.ApplicationError(
                f"Workflow class {init.workflow_type} is not registered on this worker, available workflows: {workflow_names}",
                type="NotFoundError",
            )

        # Build info
        parent: Optional[temporalio.workflow.ParentInfo] = None
        root: Optional[temporalio.workflow.RootInfo] = None
        if init.HasField("parent_workflow_info"):
            parent = temporalio.workflow.ParentInfo(
                namespace=init.parent_workflow_info.namespace,
                run_id=init.parent_workflow_info.run_id,
                workflow_id=init.parent_workflow_info.workflow_id,
            )
        if init.HasField("root_workflow"):
            root = temporalio.workflow.RootInfo(
                run_id=init.root_workflow.run_id,
                workflow_id=init.root_workflow.workflow_id,
            )
        info = temporalio.workflow.Info(
            attempt=init.attempt,
            continued_run_id=init.continued_from_execution_run_id or None,
            cron_schedule=init.cron_schedule or None,
            execution_timeout=init.workflow_execution_timeout.ToTimedelta()
            if init.HasField("workflow_execution_timeout")
            else None,
            first_execution_run_id=init.first_execution_run_id,
            headers=dict(init.headers),
            namespace=self._namespace,
            parent=parent,
            root=root,
            raw_memo=dict(init.memo.fields),
            retry_policy=temporalio.common.RetryPolicy.from_proto(init.retry_policy)
            if init.HasField("retry_policy")
            else None,
            run_id=act.run_id,
            run_timeout=init.workflow_run_timeout.ToTimedelta()
            if init.HasField("workflow_run_timeout")
            else None,
            search_attributes=temporalio.converter.decode_search_attributes(
                init.search_attributes
            ),
            start_time=act.timestamp.ToDatetime().replace(tzinfo=timezone.utc),
            workflow_start_time=init.start_time.ToDatetime().replace(
                tzinfo=timezone.utc
            ),
            task_queue=self._task_queue,
            task_timeout=init.workflow_task_timeout.ToTimedelta(),
            typed_search_attributes=temporalio.converter.decode_typed_search_attributes(
                init.search_attributes
            ),
            workflow_id=init.workflow_id,
            workflow_type=init.workflow_type,
            priority=temporalio.common.Priority._from_proto(init.priority),
        )

        # Create instance from details
        det = WorkflowInstanceDetails(
            payload_converter_class=self._data_converter.payload_converter_class,
            failure_converter_class=self._data_converter.failure_converter_class,
            interceptor_classes=self._interceptor_classes,
            defn=defn,
            info=info,
            randomness_seed=init.randomness_seed,
            extern_functions=self._extern_functions,
            disable_eager_activity_execution=self._disable_eager_activity_execution,
            worker_level_failure_exception_types=self._workflow_failure_exception_types,
        )
        if defn.sandboxed:
            return self._workflow_runner.create_instance(det)
        else:
            return self._unsandboxed_workflow_runner.create_instance(det)

    def nondeterminism_as_workflow_fail(self) -> bool:
        return any(
            issubclass(temporalio.workflow.NondeterminismError, typ)
            for typ in self._workflow_failure_exception_types
        )

    def nondeterminism_as_workflow_fail_for_types(self) -> Set[str]:
        return set(
            k
            for k, v in self._workflows.items()
            if any(
                issubclass(temporalio.workflow.NondeterminismError, typ)
                for typ in v.failure_exception_types
            )
        )


class _DeadlockError(Exception):
    """Exception class for deadlocks. Contains functionality to swap the default traceback for another."""

    def __init__(self, message: str, replacement_tb: Optional[TracebackType] = None):
        """Create a new DeadlockError, with message `message` and optionally a traceback `replacement_tb` to be swapped in later.

        Args:
            message: Message to be presented through exception.
            replacement_tb: Optional TracebackType to be swapped later.
        """
        super().__init__(message)
        self._new_tb = replacement_tb

    def swap_traceback(self) -> None:
        """Swap the current traceback for the replacement passed during construction. Used to work around Python adding the current frame to the stack trace.

        Returns:
            None
        """
        if self._new_tb:
            self.__traceback__ = self._new_tb
            self._new_tb = None

    @classmethod
    def from_deadlocked_workflow(
        cls, workflow: WorkflowInstance, timeout: Optional[int]
    ):
        msg = f"[TMPRL1101] Potential deadlock detected: workflow didn't yield within {timeout} second(s)."
        tid = workflow.get_thread_id()
        if not tid:
            return cls(msg)

        try:
            tb = cls._gen_tb_helper(tid)
            if tb:
                return cls(msg, tb)
            return cls(f"{msg} (no frames available)")
        except Exception as err:
            return cls(f"{msg} (failed getting frames: {err})")

    @staticmethod
    def _gen_tb_helper(
        tid: int,
    ) -> Optional[TracebackType]:
        """Take a thread id and construct a stack trace.

        Returns:
            <Optional[TracebackType]> the traceback that was constructed, None if the thread could not be found.
        """
        frame = sys._current_frames().get(tid)
        if not frame:
            return None

        # not using traceback.extract_stack() because it obfuscates the frame objects (specifically f_lasti)
        thread_frames = [frame]
        while frame.f_back:
            frame = frame.f_back
            thread_frames.append(frame)

        thread_frames.reverse()

        size = 0
        tb = None
        for frm in thread_frames:
            tb = TracebackType(tb, frm, frm.f_lasti, frm.f_lineno)
            size += sys.getsizeof(tb)

        while size > 200000 and tb:
            size -= sys.getsizeof(tb)
            tb = tb.tb_next

        return tb


class _RunningWorkflow:
    def __init__(self, instance: WorkflowInstance):
        self.instance = instance
        self.deadlocked_activation_task: Optional[Awaitable] = None
        self._deadlock_can_be_interrupted_lock = threading.Lock()
        self._deadlock_can_be_interrupted = False

    def activate(
        self, act: temporalio.bridge.proto.workflow_activation.WorkflowActivation
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        # Mark that the deadlock can be interrupted, do work, then unmark
        with self._deadlock_can_be_interrupted_lock:
            self._deadlock_can_be_interrupted = True
        try:
            return self.instance.activate(act)
        finally:
            with self._deadlock_can_be_interrupted_lock:
                self._deadlock_can_be_interrupted = False

    def attempt_deadlock_interruption(self) -> None:
        # Need to be under mutex to ensure it can be interrupted
        with self._deadlock_can_be_interrupted_lock:
            # Do not interrupt if cannot be interrupted anymore
            if not self._deadlock_can_be_interrupted:
                return
            deadlocked_thread_id = self.instance.get_thread_id()
            if deadlocked_thread_id:
                temporalio.bridge.runtime.Runtime._raise_in_thread(
                    deadlocked_thread_id, _InterruptDeadlockError
                )


class _InterruptDeadlockError(BaseException):
    pass
