"""Replayer."""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass

from typing_extensions import TypedDict

import temporalio.api.history.v1
import temporalio.bridge.proto.workflow_activation
import temporalio.bridge.worker
import temporalio.client
import temporalio.converter
import temporalio.runtime
import temporalio.worker
import temporalio.workflow

from ..common import HeaderCodecBehavior
from ._interceptor import Interceptor
from ._worker import load_default_build_id
from ._workflow import _WorkflowWorker
from ._workflow_instance import (
    _DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS,
    UnsandboxedWorkflowRunner,
    WorkflowRunner,
    _WorkflowLogicFlag,
)
from .workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)


class Replayer:
    """Replayer to replay workflows from history."""

    def __init__(
        self,
        *,
        workflows: Sequence[type],
        workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        workflow_runner: WorkflowRunner = SandboxedWorkflowRunner(),
        unsandboxed_workflow_runner: WorkflowRunner = UnsandboxedWorkflowRunner(),
        namespace: str = "ReplayNamespace",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        plugins: Sequence[temporalio.worker.Plugin] = [],
        build_id: str | None = None,
        identity: str | None = None,
        workflow_failure_exception_types: Sequence[type[BaseException]] = [],
        debug_mode: bool = False,
        runtime: temporalio.runtime.Runtime | None = None,
        disable_safe_workflow_eviction: bool = False,
        header_codec_behavior: HeaderCodecBehavior = HeaderCodecBehavior.NO_CODEC,
    ) -> None:
        """Create a replayer to replay workflows from history.

        See :py:meth:`temporalio.worker.Worker.__init__` for a description of
        most of the arguments. Most of the same arguments need to be passed to
        the replayer that were passed to the worker when the workflow originally
        ran.

        Note, unlike the worker, for the replayer the workflow_task_executor
        will default to a new thread pool executor with no max_workers set that
        will be shared across all replay calls and never explicitly shut down.
        Users are encouraged to provide their own if needing more control.
        """
        self._config = ReplayerConfig(
            workflows=list(workflows),
            workflow_task_executor=(
                workflow_task_executor or concurrent.futures.ThreadPoolExecutor()
            ),
            workflow_runner=workflow_runner,
            unsandboxed_workflow_runner=unsandboxed_workflow_runner,
            namespace=namespace,
            data_converter=data_converter,
            interceptors=interceptors,
            build_id=build_id,
            identity=identity,
            workflow_failure_exception_types=workflow_failure_exception_types,
            debug_mode=debug_mode,
            runtime=runtime,
            disable_safe_workflow_eviction=disable_safe_workflow_eviction,
            header_codec_behavior=header_codec_behavior,
        )
        self._initial_config = self._config.copy()
        self._default_workflow_logic_flags = set(_DEFAULT_ENABLED_WORKFLOW_LOGIC_FLAGS)

        # Apply plugin configuration
        self.plugins = plugins
        for plugin in plugins:
            self._config = plugin.configure_replayer(self._config)

        # Validate workflows after plugin configuration
        if not self._config.get("workflows"):
            raise ValueError("At least one workflow must be specified")

    def _set_default_workflow_logic_flag(
        self, flag: _WorkflowLogicFlag, *, enabled: bool
    ) -> None:
        if enabled:
            self._default_workflow_logic_flags.add(flag)
        else:
            self._default_workflow_logic_flags.discard(flag)

    def config(self, *, active_config: bool = False) -> ReplayerConfig:
        """Config, as a dictionary, used to create this replayer.

        Args:
            active_config: If true, return the modified configuration in use rather than the initial one
                provided to the client.

        Returns:
            Configuration, shallow-copied.
        """
        config = self._config.copy() if active_config else self._initial_config.copy()
        config["workflows"] = list(config.get("workflows", []))
        return config

    async def replay_workflow(
        self,
        history: temporalio.client.WorkflowHistory,
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResult:
        """Replay a workflow for the given history.

        Args:
            history: The history to replay. Can be fetched directly, or use
                :py:meth:`temporalio.client.WorkflowHistory.from_json` to parse
                a history downloaded via ``Temporal CLI`` or the web UI.
            raise_on_replay_failure: If ``True`` (the default), this will raise
                a :py:attr:`WorkflowReplayResult.replay_failure` if it is
                present.
        """

        async def history_iterator():
            yield history

        async with self.workflow_replay_iterator(history_iterator()) as replay_iterator:
            async for result in replay_iterator:
                if raise_on_replay_failure and result.replay_failure:
                    raise result.replay_failure
                return result
            # Should never be reached
            raise RuntimeError("No histories")

    async def replay_workflows(
        self,
        histories: AsyncIterator[temporalio.client.WorkflowHistory],
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResults:
        """Replay workflows for the given histories.

        This is a shortcut for :py:meth:`workflow_replay_iterator` that iterates
        all results and aggregates information about them.

        Args:
            histories: The histories to replay, from an async iterator.
            raise_on_replay_failure: If ``True`` (the default), this will raise
                the first replay failure seen.

        Returns:
            Aggregated results.
        """
        async with self.workflow_replay_iterator(histories) as replay_iterator:
            replay_failures: dict[str, Exception] = {}
            async for result in replay_iterator:
                if result.replay_failure:
                    if raise_on_replay_failure:
                        raise result.replay_failure
                    replay_failures[result.history.run_id] = result.replay_failure
            return WorkflowReplayResults(replay_failures=replay_failures)

    def workflow_replay_iterator(
        self, histories: AsyncIterator[temporalio.client.WorkflowHistory]
    ) -> AbstractAsyncContextManager[AsyncIterator[WorkflowReplayResult]]:
        """Replay workflows for the given histories.

        This is a context manager for use via ``async with``. The value is an
        iterator for use via ``async for``.

        Args:
            histories: The histories to replay, from an async iterator.

        Returns:
            An async iterator that returns replayed workflow results as they are
            replayed.
        """

        def make_lambda(plugin, next):  # type: ignore[reportMissingParameterType]
            return lambda r, hs: plugin.run_replayer(r, hs, next)

        next_function = lambda r, hs: r._workflow_replay_iterator(hs)
        for plugin in reversed(self.plugins):
            next_function = make_lambda(plugin, next_function)

        return next_function(self, histories)

    @asynccontextmanager
    async def _workflow_replay_iterator(
        self, histories: AsyncIterator[temporalio.client.WorkflowHistory]
    ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        # Initialize variables to avoid unbound variable errors
        pusher = None
        workflow_worker_task = None
        bridge_worker_scope = None

        try:
            last_replay_failure: Exception | None
            last_replay_complete = asyncio.Event()

            # Create eviction hook
            def on_eviction_hook(
                _run_id: str,
                remove_job: temporalio.bridge.proto.workflow_activation.RemoveFromCache,
            ) -> None:
                nonlocal last_replay_failure
                if (
                    remove_job.reason
                    == temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.NONDETERMINISM
                ):
                    last_replay_failure = temporalio.workflow.NondeterminismError(
                        remove_job.message
                    )
                elif (
                    remove_job.reason
                    != temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.CACHE_FULL
                    and remove_job.reason
                    != temporalio.bridge.proto.workflow_activation.RemoveFromCache.EvictionReason.LANG_REQUESTED
                ):
                    last_replay_failure = RuntimeError(
                        f"{remove_job.reason}: {remove_job.message}"
                    )
                else:
                    last_replay_failure = None
                last_replay_complete.set()

            # Create worker referencing bridge worker
            bridge_worker: temporalio.bridge.worker.Worker
            task_queue = f"replay-{self._config.get('build_id')}"
            runtime = (
                self._config.get("runtime") or temporalio.runtime.Runtime.default()
            )
            data_converter = (
                self._config.get("data_converter")
                or temporalio.converter.DataConverter.default
            )
            workflow_worker = _WorkflowWorker(
                bridge_worker=lambda: bridge_worker,
                namespace=self._config.get("namespace", "ReplayNamespace"),
                task_queue=task_queue,
                workflows=self._config.get("workflows", []),
                workflow_task_executor=self._config.get("workflow_task_executor"),
                max_concurrent_workflow_tasks=5,
                workflow_runner=self._config.get("workflow_runner")
                or SandboxedWorkflowRunner(),
                unsandboxed_workflow_runner=self._config.get(
                    "unsandboxed_workflow_runner"
                )
                or UnsandboxedWorkflowRunner(),
                data_converter=data_converter,
                interceptors=self._config.get("interceptors", []),
                workflow_failure_exception_types=self._config.get(
                    "workflow_failure_exception_types", []
                ),
                patch_activation_callback=None,
                debug_mode=self._config.get("debug_mode", False),
                metric_meter=runtime.metric_meter,
                on_eviction_hook=on_eviction_hook,
                disable_eager_activity_execution=False,
                disable_safe_eviction=self._config.get(
                    "disable_safe_workflow_eviction", False
                ),
                should_enforce_versioning_behavior=False,
                assert_local_activity_valid=lambda a: None,
                encode_headers=self._config.get(
                    "header_codec_behavior", HeaderCodecBehavior.NO_CODEC
                )
                != HeaderCodecBehavior.NO_CODEC,
                max_workflow_task_external_storage_concurrency=1,
                default_workflow_logic_flags=frozenset(
                    self._default_workflow_logic_flags
                ),
            )
            external_storage = data_converter.external_storage
            storage_driver_types = (
                {driver.type() for driver in external_storage.drivers}
                if external_storage
                else set()
            )

            # Create bridge worker
            bridge_worker, pusher = temporalio.bridge.worker.Worker.for_replay(
                runtime._core_runtime,
                temporalio.bridge.worker.WorkerConfig(
                    namespace=self._config.get("namespace", "ReplayNamespace"),
                    task_queue=task_queue,
                    identity_override=self._config.get("identity"),
                    # Need to tell core whether we want to consider all
                    # non-determinism exceptions as workflow fail, and whether we do
                    # per workflow type
                    nondeterminism_as_workflow_fail=workflow_worker.nondeterminism_as_workflow_fail(),
                    nondeterminism_as_workflow_fail_for_types=workflow_worker.nondeterminism_as_workflow_fail_for_types(),
                    # All values below are ignored but required by Core
                    max_cached_workflows=2,
                    tuner=temporalio.bridge.worker.TunerHolder(
                        workflow_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            2
                        ),
                        activity_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                        local_activity_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                        nexus_slot_supplier=temporalio.bridge.worker.FixedSizeSlotSupplier(
                            1
                        ),
                    ),
                    nonsticky_to_sticky_poll_ratio=1,
                    no_remote_activities=True,
                    disable_payload_error_limit=True,
                    task_types=temporalio.bridge.worker.WorkerTaskTypes(
                        enable_workflows=True,
                        enable_local_activities=False,
                        enable_remote_activities=False,
                        enable_nexus=False,
                    ),
                    sticky_queue_schedule_to_start_timeout_millis=1000,
                    max_heartbeat_throttle_interval_millis=1000,
                    default_heartbeat_throttle_interval_millis=1000,
                    max_activities_per_second=None,
                    max_task_queue_activities_per_second=None,
                    max_eager_activity_reservations_per_workflow_task=3,
                    graceful_shutdown_period_millis=0,
                    versioning_strategy=temporalio.bridge.worker.WorkerVersioningStrategyNone(
                        build_id_no_versioning=self._config.get("build_id")
                        or load_default_build_id(),
                    ),
                    workflow_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        2
                    ),
                    activity_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        1
                    ),
                    nexus_task_poller_behavior=temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
                        1
                    ),
                    plugins=[plugin.name() for plugin in self.plugins],
                    storage_drivers=storage_driver_types,
                ),
            )
            bridge_worker_scope = bridge_worker

            # Start worker
            workflow_worker_task = asyncio.create_task(workflow_worker.run())

            # Yield iterator
            async def replay_iterator() -> AsyncIterator[WorkflowReplayResult]:
                async for history in histories:
                    # Clear last complete and push history
                    last_replay_complete.clear()
                    await pusher.push_history(
                        history.workflow_id,
                        temporalio.api.history.v1.History(
                            events=history.events
                        ).SerializeToString(),
                    )

                    # Wait for worker error or last replay to complete. This
                    # should never take more than a few seconds due to deadlock
                    # detector but we cannot add timeout just in case debug mode
                    # is enabled.
                    await asyncio.wait(  # type: ignore
                        [
                            workflow_worker_task,
                            asyncio.create_task(last_replay_complete.wait()),
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    # If worker task complete, wait on it so it'll throw
                    if workflow_worker_task.done():
                        await workflow_worker_task
                    # Should always be set if workflow worker didn't throw
                    assert last_replay_complete.is_set()

                    yield WorkflowReplayResult(
                        history=history,
                        replay_failure=last_replay_failure,
                    )

            yield replay_iterator()
        finally:
            # Close the pusher
            if pusher is not None:
                pusher.close()
            # If the workflow worker task is not done, wait for it
            try:
                if workflow_worker_task is not None and not workflow_worker_task.done():
                    await workflow_worker_task
            except Exception:
                logger.warning("Failed to shutdown worker", exc_info=True)
            finally:
                # We must shutdown here
                try:
                    if bridge_worker_scope is not None:
                        bridge_worker_scope.initiate_shutdown()
                        await bridge_worker_scope.finalize_shutdown()
                except Exception:
                    logger.warning("Failed to finalize shutdown", exc_info=True)


class ReplayerConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:class:`Replayer`."""

    workflows: Sequence[type]
    workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None
    workflow_runner: WorkflowRunner
    unsandboxed_workflow_runner: WorkflowRunner
    namespace: str
    data_converter: temporalio.converter.DataConverter
    interceptors: Sequence[Interceptor]
    build_id: str | None
    identity: str | None
    workflow_failure_exception_types: Sequence[type[BaseException]]
    debug_mode: bool
    runtime: temporalio.runtime.Runtime | None
    disable_safe_workflow_eviction: bool
    header_codec_behavior: HeaderCodecBehavior


@dataclass(frozen=True)
class WorkflowReplayResult:
    """Single workflow replay result."""

    history: temporalio.client.WorkflowHistory
    """History originally passed for this workflow replay."""

    replay_failure: Exception | None
    """Failure during replay if any.

    This does not mean your workflow exited by raising an error, but rather that
    some task failure such as
    :py:class:`temporalio.workflow.NondeterminismError` was encountered during
    replay - likely indicating your workflow code is incompatible with the
    history.
    """


@dataclass(frozen=True)
class WorkflowReplayResults:
    """Results of replaying multiple workflows."""

    replay_failures: Mapping[str, Exception]
    """Replay failures, keyed by run ID."""
