"""Workflow worker."""

from __future__ import annotations

import asyncio
import concurrent.futures
import dataclasses
import logging
import os
import sys
import threading
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import timezone
from types import TracebackType

import temporalio.api.common.v1
import temporalio.bridge.proto.common
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.proto.workflow_completion
import temporalio.bridge.runtime
import temporalio.bridge.worker
import temporalio.common
import temporalio.converter
import temporalio.converter._extstore
import temporalio.exceptions
import temporalio.workflow
from temporalio.bridge.worker import PollShutdownError
from temporalio.converter import StorageDriverStoreContext, StorageDriverWorkflowInfo
from temporalio.worker.workflow_sandbox._runner import SandboxedWorkflowRunner

from . import _command_aware_visitor
from ._debugger import (
    _install_workflow_breakpoint_hook,
    _relax_sandbox_for_debugger,
)
from ._interceptor import (
    Interceptor,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
)
from ._workflow_instance import (
    _DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS,
    PatchActivationInput,
    WorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowRunner,
    _WorkflowExternFunctions,
    _WorkflowLogicFlag,
)

logger = logging.getLogger(__name__)

# Set to true to log all activations and completions
LOG_PROTOS = False


# Value was chosen abitrarily as a small number that allows some concurrency and prevents
# large numbers of concurrent external storage operations causing resource contention.
# This default limit is per workflow task activation and does not limit the total number
# of concurrent external storage operations across all workflow task activations.
# Advise customers to adjust based on their workload needs and to report issues with the
# value if problems are encountered. This setting is experimental.
_DEFAULT_WORKFLOW_TASK_EXTERNAL_STORAGE_CONCURRENCY: int = 3


def _set_external_storage_metrics(
    target: temporalio.bridge.proto.common.ExternalStorageMetrics,
    metrics: temporalio.converter._extstore.StorageOperationMetrics,
) -> None:
    """Populate a proto ``ExternalStorageMetrics`` from measured storage metrics."""
    target.payload_count = metrics.payload_count
    target.total_size_bytes = metrics.total_size
    target.total_duration.FromTimedelta(metrics.total_duration)
    target.driver_names.extend(sorted(metrics.driver_names))


class _WorkflowWorker:  # type:ignore[reportUnusedClass]
    def __init__(
        self,
        *,
        bridge_worker: Callable[[], temporalio.bridge.worker.Worker],
        namespace: str,
        task_queue: str,
        workflows: Sequence[type],
        workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None,
        max_concurrent_workflow_tasks: int | None,
        workflow_runner: WorkflowRunner,
        unsandboxed_workflow_runner: WorkflowRunner,
        data_converter: temporalio.converter.DataConverter,
        interceptors: Sequence[Interceptor],
        workflow_failure_exception_types: Sequence[type[BaseException]],
        patch_activation_callback: Callable[[PatchActivationInput], bool] | None,
        debug_mode: bool,
        disable_eager_activity_execution: bool,
        metric_meter: temporalio.common.MetricMeter,
        on_eviction_hook: Callable[
            [str, temporalio.bridge.proto.workflow_activation.RemoveFromCache], None
        ]
        | None,
        disable_safe_eviction: bool,
        should_enforce_versioning_behavior: bool,
        assert_local_activity_valid: Callable[[str], None],
        encode_headers: bool,
        max_workflow_task_external_storage_concurrency: int,
        default_workflow_logic_flags: frozenset[_WorkflowLogicFlag] | None = None,
    ) -> None:
        # Debug mode is enabled if specified or if the TEMPORAL_DEBUG env var is truthy
        debug_mode = debug_mode or bool(os.environ.get("TEMPORAL_DEBUG"))

        self._bridge_worker = bridge_worker
        self._namespace = namespace
        self._task_queue = task_queue
        self._default_workflow_logic_flags = set(
            _DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS
            if default_workflow_logic_flags is None
            else default_workflow_logic_flags
        )
        self._workflow_task_executor = (
            workflow_task_executor
            or concurrent.futures.ThreadPoolExecutor(
                max_workers=max_concurrent_workflow_tasks or 500,
                thread_name_prefix="temporal_workflow_",
            )
        )
        self._workflow_task_executor_user_provided = workflow_task_executor is not None

        # If debug mode is enabled, ensure that the debugpy (https://github.com/microsoft/debugpy)
        # import is added as a passthrough
        if debug_mode and isinstance(workflow_runner, SandboxedWorkflowRunner):
            workflow_runner = dataclasses.replace(
                workflow_runner,
                restrictions=workflow_runner.restrictions.with_passthrough_modules(
                    "_pydevd_bundle"
                ),
            )

        # In debug mode, also lift the sandbox restriction on breakpoint()
        # and install the workflow-aware breakpoint hook so pdb works in
        # workflow code. Outside of debug mode neither happens.
        self._debug_mode = debug_mode
        if self._debug_mode:
            workflow_runner = _relax_sandbox_for_debugger(workflow_runner)
            _install_workflow_breakpoint_hook()
        self._workflow_runner = workflow_runner

        self._unsandboxed_workflow_runner = unsandboxed_workflow_runner
        self._data_converter = data_converter
        # Build the interceptor classes and collect extern functions
        self._extern_functions: MutableMapping[str, Callable] = {}
        self._interceptor_classes: list[type[WorkflowInboundInterceptor]] = []
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
        self._patch_activation_callback = patch_activation_callback
        self._running_workflows: dict[str, _RunningWorkflow] = {}
        self._disable_eager_activity_execution = disable_eager_activity_execution
        self._on_eviction_hook = on_eviction_hook
        self._disable_safe_eviction = disable_safe_eviction
        self._encode_headers = encode_headers
        self._max_workflow_task_external_storage_concurrency = (
            max_workflow_task_external_storage_concurrency
        )
        self._throw_after_activation: Exception | None = None

        # If debug mode is enabled, disable deadlock detection
        # otherwise set to 2 seconds
        self._deadlock_timeout_seconds = None if self._debug_mode else 2

        # Keep track of workflows that could not be evicted
        self._could_not_evict_count = 0

        # Set the worker-level failure exception types into the runner
        workflow_runner.set_worker_level_failure_exception_types(
            workflow_failure_exception_types
        )

        # Validate and build workflow dict
        self._workflows: dict[str, temporalio.workflow._Definition] = {}
        self._dynamic_workflow: temporalio.workflow._Definition | None = None
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
        except PollShutdownError:
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
            except PollShutdownError:
                return

    async def _activate_inline_for_debug(
        self,
        loop: asyncio.AbstractEventLoop,
        workflow: _RunningWorkflow,
        act: temporalio.bridge.proto.workflow_activation.WorkflowActivation,
    ) -> temporalio.bridge.proto.workflow_completion.WorkflowActivationCompletion:
        # Indirect through call_soon + a future so the activation runs outside
        # the dispatch task's __step() context. Python 3.14 refuses to enter a
        # task while another on the same thread is mid-step; suspending at the
        # await below clears that state so workflow.activate can step its own
        # task without collision.
        future: asyncio.Future = loop.create_future()

        def run_inline() -> None:
            # _run_once clears the running-loop registration on exit; restore
            # the main loop so later code sees the right one.
            main_loop = asyncio._get_running_loop()
            try:
                completion = workflow.activate(act)
                future.set_result(completion)
            except BaseException as e:
                future.set_exception(e)
            finally:
                asyncio._set_running_loop(main_loop)

        loop.call_soon(run_inline)
        return await future

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
        workflow = None
        data_converter = self._data_converter
        download_metrics = temporalio.converter._extstore.StorageOperationMetrics()
        try:
            if LOG_PROTOS:
                logger.debug("Received workflow activation:\n%s", act)

            workflow = self._running_workflows.get(act.run_id)
            if not workflow:
                if not init_job:
                    raise RuntimeError(
                        "Missing initialize workflow, workflow could have unexpectedly been removed from cache"
                    )
                workflow_id = init_job.workflow_id
            else:
                workflow_id = workflow.workflow_id
                if init_job:
                    # Should never happen
                    logger.warning(
                        "Cache already exists for activation with initialize job"
                    )

            workflow_context = temporalio.converter.WorkflowSerializationContext(
                namespace=self._namespace,
                workflow_id=workflow_id,
            )
            data_converter = self._data_converter._with_contexts(
                workflow_context,
                StorageDriverStoreContext(
                    target=StorageDriverWorkflowInfo(
                        id=workflow_id,
                        run_id=act.run_id,
                        type=(
                            workflow.get_info().workflow_type
                            if workflow
                            else (init_job.workflow_type if init_job else None)
                        ),
                        namespace=self._namespace,
                    ),
                ),
            )
            if workflow:
                data_converter = _CommandAwareDataConverter.create(
                    instance=workflow.instance,
                    context_free_dc=self._data_converter,
                    workflow_context_dc=data_converter,
                    workflow_context=workflow_context,
                )
            download_metrics = await temporalio.bridge.worker.decode_activation(
                act,
                data_converter,
                decode_headers=self._encode_headers,
                storage_concurrency_limit=self._max_workflow_task_external_storage_concurrency,
            )
            if not workflow:
                assert init_job
                workflow = _RunningWorkflow(
                    self._create_workflow_instance(act, init_job), workflow_id
                )
                self._running_workflows[act.run_id] = workflow

            if self._debug_mode:
                # Inline on the main thread so pdb / breakpoint() can read
                # stdin. The loop blocks during the activation — that's the
                # intended single-stepping semantic.
                completion = await self._activate_inline_for_debug(
                    asyncio.get_running_loop(), workflow, act
                )
            else:
                # Run activation in separate thread so we can check if it's
                # deadlocked
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

            if (
                isinstance(err, temporalio.exceptions.ApplicationError)
                and err.non_retryable
            ):
                # Fail the workflow execution terminally rather than failing the task
                command = completion.successful.commands.add()
                failure = command.fail_workflow_execution.failure
                failure.SetInParent()
                try:
                    data_converter.failure_converter.to_failure(
                        err,
                        data_converter.payload_converter,
                        failure,
                    )
                except Exception as inner_err:
                    logger.exception(
                        "Failed converting activation exception on workflow with run ID %s",
                        act.run_id,
                    )
                    failure.message = (
                        f"Failed converting activation exception: {inner_err}"
                    )
            else:
                completion.failed.failure.SetInParent()
                try:
                    data_converter.failure_converter.to_failure(
                        err,
                        data_converter.payload_converter,
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

        completion.run_id = act.run_id

        # Encode completion
        if workflow:
            workflow_context = temporalio.converter.WorkflowSerializationContext(
                namespace=self._namespace,
                workflow_id=workflow.workflow_id,
            )
            data_converter = _CommandAwareDataConverter.create(
                instance=workflow.instance,
                context_free_dc=self._data_converter,
                workflow_context_dc=self._data_converter.with_context(workflow_context),
                workflow_context=workflow_context,
            )

        upload_metrics = temporalio.converter._extstore.StorageOperationMetrics()
        try:
            upload_metrics = await temporalio.bridge.worker.encode_completion(
                completion,
                data_converter,
                encode_headers=self._encode_headers,
                storage_concurrency_limit=self._max_workflow_task_external_storage_concurrency,
            )
        except Exception as err:
            logger.exception(
                "Failed encoding completion on workflow with run ID %s", act.run_id
            )
            completion.failed.Clear()
            completion.failed.failure.message = f"Failed encoding completion: {err}"

        # Reported on the completion so core can include them in its workflow-task duration
        # log; core measures the duration itself.
        if download_metrics.payload_count > 0:
            _set_external_storage_metrics(
                completion.payload_download_metrics, download_metrics
            )
        if upload_metrics.payload_count > 0:
            _set_external_storage_metrics(
                completion.payload_upload_metrics, upload_metrics
            )

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
            handle_eviction_task: asyncio.Future | None = None
            while True:
                try:
                    if self._debug_mode:
                        await self._activate_inline_for_debug(
                            asyncio.get_running_loop(), workflow, act
                        )
                    else:
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
        parent: temporalio.workflow.ParentInfo | None = None
        root: temporalio.workflow.RootInfo | None = None
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

        last_failure = (
            init.continued_failure if init.HasField("continued_failure") else None
        )

        # Create instance from details
        det = WorkflowInstanceDetails(
            payload_converter_factory=self._data_converter._new_payload_converter,
            failure_converter_class=self._data_converter.failure_converter_class,
            interceptor_classes=self._interceptor_classes,
            defn=defn,
            info=info,
            randomness_seed=init.randomness_seed,
            extern_functions=self._extern_functions,
            disable_eager_activity_execution=self._disable_eager_activity_execution,
            worker_level_failure_exception_types=self._workflow_failure_exception_types,
            patch_activation_callback=self._patch_activation_callback,
            last_completion_result=init.last_completion_result,
            last_failure=last_failure,
            default_workflow_logic_flags=frozenset(self._default_workflow_logic_flags),
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

    def _set_default_workflow_logic_flag(
        self, flag: _WorkflowLogicFlag, *, enabled: bool
    ) -> None:
        if enabled:
            self._default_workflow_logic_flags.add(flag)
        else:
            self._default_workflow_logic_flags.discard(flag)

    def nondeterminism_as_workflow_fail_for_types(self) -> set[str]:
        return {
            k
            for k, v in self._workflows.items()
            if any(
                issubclass(temporalio.workflow.NondeterminismError, typ)
                for typ in v.failure_exception_types
            )
        }


class _DeadlockError(Exception):
    """Exception class for deadlocks. Contains functionality to swap the default traceback for another."""

    def __init__(self, message: str, replacement_tb: TracebackType | None = None):
        """Create a new DeadlockError, with message ``message`` and optionally a traceback ``replacement_tb`` to be swapped in later.

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
    def from_deadlocked_workflow(cls, workflow: WorkflowInstance, timeout: int | None):
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
    ) -> TracebackType | None:
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
    def __init__(
        self,
        instance: WorkflowInstance,
        workflow_id: str,
    ):
        self.instance = instance
        self.workflow_id = workflow_id
        self.deadlocked_activation_task: Awaitable | None = None
        self._deadlock_can_be_interrupted_lock = threading.Lock()
        self._deadlock_can_be_interrupted = False

    def get_info(self) -> temporalio.workflow.Info:
        return self.instance.get_info()

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


@dataclass(frozen=True)
class _CommandAwareDataConverter(temporalio.converter.DataConverter):
    """Data converter that resolves serialization context per-command.

    Responds to the context variable set by
    :py:class:`_command_aware_visitor.CommandAwarePayloadVisitor`.
    """

    _ca_instance: WorkflowInstance = dataclasses.field(
        default=None,
        repr=False,
        compare=False,  # type: ignore[assignment]
    )
    _ca_context_free_dc: temporalio.converter.DataConverter = dataclasses.field(
        default=None,
        repr=False,
        compare=False,  # type: ignore[assignment]
    )
    _ca_workflow_context_dc: temporalio.converter.DataConverter = dataclasses.field(
        default=None,
        repr=False,
        compare=False,  # type: ignore[assignment]
    )
    _ca_workflow_context: temporalio.converter.WorkflowSerializationContext = (
        dataclasses.field(
            default=None,
            repr=False,
            compare=False,  # type: ignore[assignment]
        )
    )

    @staticmethod
    def create(
        instance: WorkflowInstance,
        context_free_dc: temporalio.converter.DataConverter,
        workflow_context_dc: temporalio.converter.DataConverter,
        workflow_context: temporalio.converter.WorkflowSerializationContext,
    ) -> _CommandAwareDataConverter:
        return _CommandAwareDataConverter(
            payload_converter_class=workflow_context_dc.payload_converter_class,
            payload_codec=workflow_context_dc.payload_codec,
            failure_converter_class=workflow_context_dc.failure_converter_class,
            external_storage=workflow_context_dc.external_storage,
            _ca_instance=instance,
            _ca_context_free_dc=context_free_dc,
            _ca_workflow_context_dc=workflow_context_dc,
            _ca_workflow_context=workflow_context,
        )

    def _get_current_dc(self) -> temporalio.converter.DataConverter:
        context = self._ca_instance.get_serialization_context(
            _command_aware_visitor.current_command_info.get(),
        )
        if context is None:
            return self._ca_context_free_dc
        if context == self._ca_workflow_context:
            return self._ca_workflow_context_dc
        return self._ca_context_free_dc.with_context(context)

    async def _encode_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return await self._get_current_dc()._encode_payload_sequence(payloads)

    async def _external_store_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        command_info = _command_aware_visitor.current_command_info.get()
        store_ctx = self._ca_instance.get_external_store_context(command_info)
        dc = self._get_current_dc()._with_store_context(store_ctx)
        return await dc._external_store_payload_sequence(payloads)

    async def _external_retrieve_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return await self._get_current_dc()._external_retrieve_payload_sequence(
            payloads
        )

    async def _decode_payload_sequence(
        self, payloads: Sequence[temporalio.api.common.v1.Payload]
    ) -> list[temporalio.api.common.v1.Payload]:
        return await self._get_current_dc()._decode_payload_sequence(payloads)


class _InterruptDeadlockError(BaseException):
    pass
