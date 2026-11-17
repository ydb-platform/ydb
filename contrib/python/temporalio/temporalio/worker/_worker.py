"""Worker for processing Temporal workflows and/or activities."""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import logging
import sys
import warnings
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import (
    Any,
    TypeAlias,
    cast,
)

from typing_extensions import TypedDict

import temporalio.bridge.worker
import temporalio.client
import temporalio.common
import temporalio.runtime
import temporalio.service
from temporalio.common import (
    HeaderCodecBehavior,
    VersioningBehavior,
    WorkerDeploymentVersion,
)

from ._activity import SharedStateManager, _ActivityWorker
from ._interceptor import Interceptor
from ._nexus import _NexusWorker
from ._plugin import Plugin
from ._tuning import WorkerTuner
from ._workflow import (
    _DEFAULT_WORKFLOW_TASK_EXTERNAL_STORAGE_CONCURRENCY,
    _WorkflowWorker,
)
from ._workflow_instance import (
    PatchActivationInput,
    UnsandboxedWorkflowRunner,
    WorkflowRunner,
    _WorkflowLogicFlag,
)
from .workflow_sandbox import SandboxedWorkflowRunner

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PollerBehaviorSimpleMaximum:
    """A poller behavior that will attempt to poll as long as a slot is available, up to the
    provided maximum. Cannot be less than two for workflow tasks, or one for other tasks.
    """

    maximum: int = 5

    def _to_bridge(self) -> temporalio.bridge.worker.PollerBehavior:
        return temporalio.bridge.worker.PollerBehaviorSimpleMaximum(
            simple_maximum=self.maximum
        )


@dataclass(frozen=True)
class PollerBehaviorAutoscaling:
    """A poller behavior that will automatically scale the number of pollers based on feedback
    from the server. A slot must be available before beginning polling.
    """

    minimum: int = 1
    """At least this many poll calls will always be attempted (assuming slots are available)."""
    maximum: int = 100
    """At most this many poll calls will ever be open at once. Must be >= `minimum`."""
    initial: int = 5
    """This many polls will be attempted initially before scaling kicks in. Must be between
       `minimum` and `maximum`."""

    def _to_bridge(self) -> temporalio.bridge.worker.PollerBehavior:
        return temporalio.bridge.worker.PollerBehaviorAutoscaling(
            minimum=self.minimum,
            maximum=self.maximum,
            initial=self.initial,
        )


PollerBehavior: TypeAlias = PollerBehaviorSimpleMaximum | PollerBehaviorAutoscaling


class Worker:
    """Worker to process workflows and/or activities.

    Once created, workers can be run and shutdown explicitly via :py:meth:`run`
    and :py:meth:`shutdown`. Alternatively workers can be used in an
    ``async with`` clause. See :py:meth:`__aenter__` and :py:meth:`__aexit__`
    for important details about fatal errors.
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        *,
        task_queue: str,
        activities: Sequence[Callable] = [],
        nexus_service_handlers: Sequence[Any] = [],
        workflows: Sequence[type] = [],
        activity_executor: concurrent.futures.Executor | None = None,
        workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        nexus_task_executor: concurrent.futures.ThreadPoolExecutor | None = None,
        workflow_runner: WorkflowRunner = SandboxedWorkflowRunner(),
        unsandboxed_workflow_runner: WorkflowRunner = UnsandboxedWorkflowRunner(),
        plugins: Sequence[Plugin] = [],
        interceptors: Sequence[Interceptor] = [],
        build_id: str | None = None,
        identity: str | None = None,
        max_cached_workflows: int = 1000,
        max_concurrent_workflow_tasks: int | None = None,
        max_concurrent_activities: int | None = None,
        max_concurrent_local_activities: int | None = None,
        max_concurrent_nexus_tasks: int | None = None,
        tuner: WorkerTuner | None = None,
        max_concurrent_workflow_task_polls: int | None = None,
        nonsticky_to_sticky_poll_ratio: float = 0.2,
        max_concurrent_activity_task_polls: int | None = None,
        no_remote_activities: bool = False,
        sticky_queue_schedule_to_start_timeout: timedelta = timedelta(seconds=10),
        max_heartbeat_throttle_interval: timedelta = timedelta(seconds=60),
        default_heartbeat_throttle_interval: timedelta = timedelta(seconds=30),
        max_activities_per_second: float | None = None,
        max_task_queue_activities_per_second: float | None = None,
        max_eager_activity_reservations_per_workflow_task: int = 3,
        graceful_shutdown_timeout: timedelta = timedelta(),
        workflow_failure_exception_types: Sequence[type[BaseException]] = [],
        shared_state_manager: SharedStateManager | None = None,
        debug_mode: bool = False,
        disable_eager_activity_execution: bool = False,
        on_fatal_error: Callable[[BaseException], Awaitable[None]] | None = None,
        use_worker_versioning: bool = False,
        disable_safe_workflow_eviction: bool = False,
        deployment_config: WorkerDeploymentConfig | None = None,
        patch_activation_callback: Callable[[PatchActivationInput], bool] | None = None,
        workflow_task_poller_behavior: PollerBehavior = PollerBehaviorSimpleMaximum(
            maximum=5
        ),
        activity_task_poller_behavior: PollerBehavior = PollerBehaviorSimpleMaximum(
            maximum=5
        ),
        nexus_task_poller_behavior: PollerBehavior = PollerBehaviorSimpleMaximum(
            maximum=5
        ),
        disable_payload_error_limit: bool = False,
        max_workflow_task_external_storage_concurrency: int = _DEFAULT_WORKFLOW_TASK_EXTERNAL_STORAGE_CONCURRENCY,
    ) -> None:
        """Create a worker to process workflows and/or activities.

        Args:
            client: Client to use for this worker. This is required and must be
                the :py:class:`temporalio.client.Client` instance or have a
                worker_service_client attribute with reference to the original
                client's underlying service client. This client cannot be
                "lazy".
            task_queue: Required task queue for this worker.
            activities: Activity callables decorated with
                :py:func:`@activity.defn<temporalio.activity.defn>`. Activities
                may be async functions or non-async functions.
            nexus_service_handlers: Instances of Nexus service handler classes
                decorated with :py:func:`@nexusrpc.handler.service_handler<nexusrpc.handler.service_handler>`.
            workflows: Workflow classes decorated with
                :py:func:`@workflow.defn<temporalio.workflow.defn>`.
            activity_executor: Concurrent executor to use for non-async
                activities. This is required if any activities are non-async.
                :py:class:`concurrent.futures.ThreadPoolExecutor` is
                recommended. If this is a
                :py:class:`concurrent.futures.ProcessPoolExecutor`, all
                non-async activities must be picklable. ``max_workers`` on the
                executor should at least be ``max_concurrent_activities`` or a
                warning is issued. Note, a broken-executor failure from this
                executor will cause the worker to fail and shutdown.
            workflow_task_executor: Thread pool executor for workflow tasks. If
                this is not present, a new
                :py:class:`concurrent.futures.ThreadPoolExecutor` will be
                created with ``max_workers`` set to
                ``max_concurrent_workflow_tasks`` if it is present, or 500
                otherwise. The default one will be properly shutdown, but if one
                is provided, the caller is responsible for shutting it down after
                the worker is shut down.
            nexus_task_executor: Executor to use for non-async
                Nexus operations. This is required if any operation start methods
                are non-``async def``.
            workflow_runner: Runner for workflows.
            unsandboxed_workflow_runner: Runner for workflows that opt-out of
                sandboxing.
            plugins: Collection of plugins for this worker. Any plugins already
                on the client that also implement :py:class:`temporalio.worker.Plugin` are
                prepended to this list and should not be explicitly given here
                to avoid running the plugin twice.
            interceptors: Collection of interceptors for this worker. Any
                interceptors already on the client that also implement
                :py:class:`Interceptor` are prepended to this list and should
                not be explicitly given here.
            build_id: A unique identifier for the current runtime, ideally provided as a
                representation of the complete source code. If not explicitly set, the system
                automatically generates a best-effort identifier by traversing and computing
                hashes of all modules in the codebase. In very large codebases this automatic
                process may significantly increase initialization time.
                Exclusive with `deployment_config`.
                WARNING: Deprecated. Use `deployment_config` instead.
            identity: Identity for this worker client. If unset, the client
                identity is used.
            max_cached_workflows: If nonzero, workflows will be cached and
                sticky task queues will be used.
            max_concurrent_workflow_tasks: Maximum allowed number of workflow
                tasks that will ever be given to this worker at one time. Mutually exclusive with
                ``tuner``. Must be set to at least two if ``max_cached_workflows`` is nonzero.
            max_concurrent_activities: Maximum number of activity tasks that will ever be given to
                the activity worker concurrently. Mutually exclusive with ``tuner``.
            max_concurrent_local_activities: Maximum number of local activity
                tasks that will ever be given to the activity worker concurrently. Mutually
                exclusive with ``tuner``.
            max_concurrent_nexus_tasks: Maximum number of Nexus tasks that will ever be given to
                the Nexus worker concurrently. Mutually exclusive with ``tuner``.
            tuner:  Provide a custom :py:class:`WorkerTuner`. Mutually exclusive with the
                ``max_concurrent_workflow_tasks``, ``max_concurrent_activities``,
                ``max_concurrent_local_activities``, and ``max_concurrent_nexus_tasks`` arguments.

                Defaults to fixed-size 100 slots for each slot kind if unset and none of the
                max_* arguments are provided.
            max_concurrent_workflow_task_polls: Maximum number of concurrent
                poll workflow task requests we will perform at a time on this worker's task queue.
                Must be set to at least two if ``max_cached_workflows`` is nonzero.

                If set, will override any value passed to ``workflow_task_poller_behavior``.

                WARNING: Deprecated, use ``workflow_task_poller_behavior`` instead
            nonsticky_to_sticky_poll_ratio: max_concurrent_workflow_task_polls *
                this number = the number of max pollers that will be allowed for
                the nonsticky queue when sticky tasks are enabled. If both
                defaults are used, the sticky queue will allow 4 max pollers
                while the nonsticky queue will allow one. The minimum for either
                poller is 1, so if ``max_concurrent_workflow_task_polls`` is 1
                and sticky queues are enabled, there will be 2 concurrent polls.
            max_concurrent_activity_task_polls: Maximum number of concurrent
                poll activity task requests we will perform at a time on this
                worker's task queue.

                If set, will override any value passed to ``activity_task_poller_behavior``.

                WARNING: Deprecated, use ``activity_task_poller_behavior`` instead
            no_remote_activities: If true, this worker will only handle workflow
                tasks and local activities, it will not poll for activity tasks.
            sticky_queue_schedule_to_start_timeout: How long a workflow task is
                allowed to sit on the sticky queue before it is timed out and
                moved to the non-sticky queue where it may be picked up by any
                worker.
            max_heartbeat_throttle_interval: Longest interval for throttling
                activity heartbeats.
            default_heartbeat_throttle_interval: Default interval for throttling
                activity heartbeats in case per-activity heartbeat timeout is
                unset. Otherwise, it's the per-activity heartbeat timeout * 0.8.
            max_activities_per_second: Limits the number of activities per
                second that this worker will process. The worker will not poll
                for new activities if by doing so it might receive and execute
                an activity which would cause it to exceed this limit.
            max_task_queue_activities_per_second: Sets the maximum number of
                activities per second the task queue will dispatch, controlled
                server-side. Note that this only takes effect upon an activity
                poll request. If multiple workers on the same queue have
                different values set, they will thrash with the last poller
                winning.
            max_eager_activity_reservations_per_workflow_task: Maximum number of
                activity slots that may be reserved for eager execution when
                completing a workflow task. The default is 3 and the value must
                be positive. To disable eager activity execution, set
                ``disable_eager_activity_execution`` to ``True``.
            graceful_shutdown_timeout: Amount of time after shutdown is called
                that activities are given to complete before their tasks are
                cancelled.
            workflow_failure_exception_types: The types of exceptions that, if a
                workflow-thrown exception extends, will cause the
                workflow/update to fail instead of suspending the workflow via
                task failure. These are applied in addition to ones set on the
                ``workflow.defn`` decorator. If ``Exception`` is set, it
                effectively will fail a workflow/update in all user exception
                cases. WARNING: This setting is experimental.
            shared_state_manager: Used for obtaining cross-process friendly
                synchronization primitives. This is required for non-async
                activities where the activity_executor is not a
                :py:class:`concurrent.futures.ThreadPoolExecutor`. Reuse of
                these across workers is encouraged.
            debug_mode: If true, will disable deadlock detection and may disable
                sandboxing in order to make using a debugger easier. If false
                but the environment variable ``TEMPORAL_DEBUG`` is truthy, this
                will be set to true.
            disable_eager_activity_execution: If true, will disable eager
                activity execution. Eager activity execution is an optimization
                on some servers that sends activities back to the same worker as
                the calling workflow if they can run there.
            on_fatal_error: An async function that can handle a failure before
                the worker shutdown commences. This cannot stop the shutdown and
                any exception raised is logged and ignored.
            use_worker_versioning: If true, the ``build_id`` argument must be
                specified, and this worker opts into the worker versioning
                feature. This ensures it only receives workflow tasks for
                workflows which it claims to be compatible with. For more
                information, see
                https://docs.temporal.io/workers#worker-versioning.
                Exclusive with ``deployment_config``.
                WARNING: Deprecated. Use ``deployment_config`` instead.
            disable_safe_workflow_eviction: If true, instead of letting the
                workflow collect its tasks properly, the worker will simply let
                the Python garbage collector collect the tasks. WARNING: Users
                should not set this value to true. The garbage collector will
                throw ``GeneratorExit`` in coroutines causing them to wake up
                in different threads and run ``finally`` and other code in the
                wrong workflow environment.
            deployment_config: Deployment config for the worker. Exclusive with ``build_id`` and
                ``use_worker_versioning``.
                WARNING: This is an experimental feature and may change in the future.
            patch_activation_callback: Callback to decide whether the first non-replay
                call to :py:func:`workflow.patched<temporalio.workflow.patched>` for a
                patch ID should activate that patch. The callback receives a
                :py:class:`PatchActivationInput` and must return ``True`` to activate the
                patch or ``False`` to leave it inactive.
                WARNING: This is an experimental feature and may change in the future.
            workflow_task_poller_behavior: Specify the behavior of workflow task polling.
                Defaults to a 5-poller maximum.
            activity_task_poller_behavior: Specify the behavior of activity task polling.
                Defaults to a 5-poller maximum.
            nexus_task_poller_behavior: Specify the behavior of Nexus task polling.
                Defaults to a 5-poller maximum.
            disable_payload_error_limit: If true, payload and memo error limit checks
                are disabled in the worker, allowing payloads and memos that are above
                the server error limit to be submitted to the Temporal server. If false,
                the worker will validate the size before submitting to the Temporal server,
                and cause a task failure if the size limit is exceeded. The default is False.
                See https://docs.temporal.io/troubleshooting/blob-size-limit-error for more
                details.
            max_workflow_task_external_storage_concurrency: Maximum number of
                external storage payload operations (store/retrieve) that may run
                concurrently within a single workflow task activation.
                Defaults to 3. Adjust this value based on your workload's needs.
                Please report any issues you encounter with this setting or if you
                feel the default should be changed.
                WARNING: This setting is experimental.

        """
        config = WorkerConfig(
            client=client,
            task_queue=task_queue,
            activities=activities,
            nexus_service_handlers=nexus_service_handlers,
            workflows=workflows,
            activity_executor=activity_executor,
            workflow_task_executor=workflow_task_executor,
            nexus_task_executor=nexus_task_executor,
            workflow_runner=workflow_runner,
            unsandboxed_workflow_runner=unsandboxed_workflow_runner,
            plugins=plugins,
            interceptors=interceptors,
            build_id=build_id,
            identity=identity,
            max_cached_workflows=max_cached_workflows,
            max_concurrent_workflow_tasks=max_concurrent_workflow_tasks,
            max_concurrent_activities=max_concurrent_activities,
            max_concurrent_local_activities=max_concurrent_local_activities,
            max_concurrent_nexus_tasks=max_concurrent_nexus_tasks,
            tuner=tuner,
            max_concurrent_workflow_task_polls=max_concurrent_workflow_task_polls,
            nonsticky_to_sticky_poll_ratio=nonsticky_to_sticky_poll_ratio,
            max_concurrent_activity_task_polls=max_concurrent_activity_task_polls,
            no_remote_activities=no_remote_activities,
            sticky_queue_schedule_to_start_timeout=sticky_queue_schedule_to_start_timeout,
            max_heartbeat_throttle_interval=max_heartbeat_throttle_interval,
            default_heartbeat_throttle_interval=default_heartbeat_throttle_interval,
            max_activities_per_second=max_activities_per_second,
            max_task_queue_activities_per_second=max_task_queue_activities_per_second,
            max_eager_activity_reservations_per_workflow_task=max_eager_activity_reservations_per_workflow_task,
            graceful_shutdown_timeout=graceful_shutdown_timeout,
            workflow_failure_exception_types=workflow_failure_exception_types,
            shared_state_manager=shared_state_manager,
            debug_mode=debug_mode,
            disable_eager_activity_execution=disable_eager_activity_execution,
            on_fatal_error=on_fatal_error,
            use_worker_versioning=use_worker_versioning,
            disable_safe_workflow_eviction=disable_safe_workflow_eviction,
            deployment_config=deployment_config,
            patch_activation_callback=patch_activation_callback,
            workflow_task_poller_behavior=workflow_task_poller_behavior,
            activity_task_poller_behavior=activity_task_poller_behavior,
            nexus_task_poller_behavior=nexus_task_poller_behavior,
            disable_payload_error_limit=disable_payload_error_limit,
            max_workflow_task_external_storage_concurrency=max_workflow_task_external_storage_concurrency,
        )

        plugins_from_client = cast(
            list[Plugin],
            [p for p in client.config()["plugins"] if isinstance(p, Plugin)],
        )
        for client_plugin in plugins_from_client:
            if type(client_plugin) in [type(p) for p in plugins]:
                warnings.warn(
                    f"The same plugin type {type(client_plugin)} is present from both client and worker. It may run twice and may not be the intended behavior."
                )
        plugins = plugins_from_client + list(plugins)
        self._initial_config = config.copy()

        self._plugins = plugins
        for plugin in plugins:
            config = plugin.configure_worker(config)

        self._init_from_config(client, config)

    def _init_from_config(self, client: temporalio.client.Client, config: WorkerConfig):
        """Handles post plugin initialization to ensure original arguments are not used.
        Client is safe to take separately since it can't be modified by worker plugins.
        """
        self._config = config
        if not (
            config.get("activities")
            or config.get("nexus_service_handlers")
            or config.get("workflows")
        ):
            raise ValueError(
                "At least one activity, Nexus service, or workflow must be specified"
            )
        if config.get("use_worker_versioning") and not config.get("build_id"):
            raise ValueError(
                "build_id must be specified when use_worker_versioning is True"
            )
        if config.get("deployment_config") and (
            config.get("build_id") or config.get("use_worker_versioning")
        ):
            raise ValueError(
                "deployment_config cannot be used with build_id or use_worker_versioning"
            )
        _deployment_config = config.get("deployment_config")
        if (
            _deployment_config is not None
            and not _deployment_config.use_worker_versioning
            and _deployment_config.default_versioning_behavior
            != VersioningBehavior.UNSPECIFIED
        ):
            raise ValueError(
                "default_versioning_behavior must be UNSPECIFIED when use_worker_versioning is False"
            )
        max_workflow_task_external_storage_concurrency = config.get(
            "max_workflow_task_external_storage_concurrency",
            _DEFAULT_WORKFLOW_TASK_EXTERNAL_STORAGE_CONCURRENCY,
        )
        if max_workflow_task_external_storage_concurrency < 1:
            raise ValueError(
                "max_workflow_task_external_storage_concurrency must be positive"
            )
        if config.get("max_eager_activity_reservations_per_workflow_task", 3) < 1:
            raise ValueError(
                "max_eager_activity_reservations_per_workflow_task must be positive; "
                "use disable_eager_activity_execution=True to disable eager activity execution"
            )

        # Prepend applicable client interceptors to the given ones
        client_config = config["client"].config(active_config=True)  # type: ignore[reportTypedDictNotRequiredAccess]
        interceptors_from_client = cast(
            list[Interceptor],
            [i for i in client_config["interceptors"] if isinstance(i, Interceptor)],
        )
        interceptors = interceptors_from_client + list(config["interceptors"])  # type: ignore[reportTypedDictNotRequiredAccess]

        # Extract storage drivers from the client's data converter
        _ext_storage = client_config["data_converter"].external_storage
        self._storage_drivers = list(_ext_storage.drivers) if _ext_storage else []

        # Extract the bridge service client
        bridge_client = _extract_bridge_client_for_worker(config["client"])  # type: ignore[reportTypedDictNotRequiredAccess]

        self._started = False
        self._shutdown_event = asyncio.Event()
        self._shutdown_complete_event = asyncio.Event()
        self._async_context_inner_task: asyncio.Task | None = None
        self._async_context_run_task: asyncio.Task | None = None
        self._async_context_run_exception: BaseException | None = None

        self._activity_worker: _ActivityWorker | None = None
        self._runtime = (
            bridge_client.config.runtime or temporalio.runtime.Runtime.default()
        )
        activities = config.get("activities")
        if activities:
            _warn_if_activity_executor_max_workers_is_inconsistent(config)
            self._activity_worker = _ActivityWorker(
                bridge_worker=lambda: self._bridge_worker,
                task_queue=config["task_queue"],  # type: ignore[reportTypedDictNotRequiredAccess]
                activities=activities,
                activity_executor=config.get("activity_executor"),
                shared_state_manager=config.get("shared_state_manager"),
                data_converter=client_config["data_converter"],
                interceptors=interceptors,
                metric_meter=self._runtime.metric_meter,
                client=client,
                encode_headers=(
                    client_config["header_codec_behavior"] == HeaderCodecBehavior.CODEC
                ),
            )
        self._nexus_worker: _NexusWorker | None = None
        nexus_service_handlers = config.get("nexus_service_handlers")
        if nexus_service_handlers:
            _warn_if_nexus_task_executor_max_workers_is_inconsistent(config)
            self._nexus_worker = _NexusWorker(
                bridge_worker=lambda: self._bridge_worker,
                client=config["client"],  # type: ignore[reportTypedDictNotRequiredAccess]
                namespace=client_config["namespace"],
                task_queue=config["task_queue"],  # type: ignore[reportTypedDictNotRequiredAccess]
                service_handlers=nexus_service_handlers,
                data_converter=client_config["data_converter"],
                interceptors=interceptors,
                metric_meter=self._runtime.metric_meter,
                executor=config.get("nexus_task_executor"),
            )
        self._workflow_worker: _WorkflowWorker | None = None
        workflows = config.get("workflows")
        if workflows:
            deployment_config = config.get("deployment_config")
            should_enforce_versioning_behavior = (
                deployment_config is not None
                and deployment_config.use_worker_versioning
                and deployment_config.default_versioning_behavior
                == temporalio.common.VersioningBehavior.UNSPECIFIED
            )

            def check_activity(activity: str):
                if self._activity_worker is None:
                    raise ValueError(
                        f"Activity function {activity} "
                        f"is not registered on this worker, no available activities.",
                    )
                self._activity_worker.assert_activity_valid(activity)

            self._workflow_worker = _WorkflowWorker(
                bridge_worker=lambda: self._bridge_worker,
                namespace=config["client"].namespace,  # type: ignore[reportTypedDictNotRequiredAccess]
                task_queue=config["task_queue"],  # type: ignore[reportTypedDictNotRequiredAccess]
                workflows=workflows,
                workflow_task_executor=config.get("workflow_task_executor"),
                max_concurrent_workflow_tasks=config.get(
                    "max_concurrent_workflow_tasks"
                ),
                workflow_runner=config["workflow_runner"],  # type: ignore[reportTypedDictNotRequiredAccess]
                unsandboxed_workflow_runner=config["unsandboxed_workflow_runner"],  # type: ignore[reportTypedDictNotRequiredAccess]
                data_converter=client_config["data_converter"],
                interceptors=interceptors,
                workflow_failure_exception_types=config[
                    "workflow_failure_exception_types"
                ],  # type: ignore[reportTypedDictNotRequiredAccess]
                patch_activation_callback=config.get("patch_activation_callback"),
                debug_mode=config["debug_mode"],  # type: ignore[reportTypedDictNotRequiredAccess]
                disable_eager_activity_execution=config[
                    "disable_eager_activity_execution"
                ],  # type: ignore[reportTypedDictNotRequiredAccess]
                metric_meter=self._runtime.metric_meter,
                on_eviction_hook=None,
                disable_safe_eviction=config["disable_safe_workflow_eviction"],  # type: ignore[reportTypedDictNotRequiredAccess]
                should_enforce_versioning_behavior=should_enforce_versioning_behavior,
                assert_local_activity_valid=check_activity,
                encode_headers=client_config["header_codec_behavior"]
                != HeaderCodecBehavior.NO_CODEC,
                max_workflow_task_external_storage_concurrency=max_workflow_task_external_storage_concurrency,
            )

        tuner = config.get("tuner")
        if tuner is not None:
            if (
                config.get("max_concurrent_workflow_tasks")
                or config.get("max_concurrent_activities")
                or config.get("max_concurrent_local_activities")
                or config.get("max_concurrent_nexus_tasks")
            ):
                raise ValueError(
                    "Cannot specify max_concurrent_workflow_tasks, max_concurrent_activities, "
                    "max_concurrent_local_activities, or max_concurrent_nexus_tasks when also "
                    "specifying tuner"
                )
        else:
            tuner = WorkerTuner.create_fixed(
                workflow_slots=config.get("max_concurrent_workflow_tasks"),
                activity_slots=config.get("max_concurrent_activities"),
                local_activity_slots=config.get("max_concurrent_local_activities"),
                nexus_slots=config.get("max_concurrent_nexus_tasks"),
            )

        bridge_tuner = tuner._to_bridge_tuner()

        versioning_strategy: temporalio.bridge.worker.WorkerVersioningStrategy
        deployment_config = config.get("deployment_config")
        if deployment_config:
            versioning_strategy = (
                deployment_config._to_bridge_worker_deployment_options()
            )
        elif config.get("use_worker_versioning"):
            build_id = config.get("build_id") or load_default_build_id()
            versioning_strategy = (
                temporalio.bridge.worker.WorkerVersioningStrategyLegacyBuildIdBased(
                    build_id_with_versioning=build_id
                )
            )
        else:
            build_id = config.get("build_id") or load_default_build_id()
            versioning_strategy = temporalio.bridge.worker.WorkerVersioningStrategyNone(
                build_id_no_versioning=build_id
            )

        workflow_task_poller_behavior = config["workflow_task_poller_behavior"]  # type: ignore[reportTypedDictNotRequiredAccess]
        max_workflow_polls = config.get("max_concurrent_workflow_task_polls")
        if max_workflow_polls:
            workflow_task_poller_behavior = PollerBehaviorSimpleMaximum(
                maximum=max_workflow_polls
            )
        activity_task_poller_behavior = config["activity_task_poller_behavior"]  # type: ignore[reportTypedDictNotRequiredAccess]
        max_activity_polls = config.get("max_concurrent_activity_task_polls")
        if max_activity_polls:
            activity_task_poller_behavior = PollerBehaviorSimpleMaximum(
                maximum=max_activity_polls
            )

        deduped_plugin_names = list({plugin.name() for plugin in self._plugins})
        deduped_storage_driver_types = {
            driver.type() for driver in self._storage_drivers
        }

        # Create bridge worker last. We have empirically observed that if it is
        # created before an error is raised from the activity worker
        # constructor, a deadlock/hang will occur presumably while trying to
        # free it.
        # TODO(cretz): Why does this cause a test hang when an exception is
        # thrown after it?
        assert bridge_client._bridge_client
        self._bridge_worker = temporalio.bridge.worker.Worker.create(
            bridge_client._bridge_client,
            temporalio.bridge.worker.WorkerConfig(
                namespace=config["client"].namespace,  # type: ignore[reportTypedDictNotRequiredAccess]
                task_queue=config["task_queue"],  # type: ignore[reportTypedDictNotRequiredAccess]
                identity_override=config.get("identity"),
                max_cached_workflows=config["max_cached_workflows"],  # type: ignore[reportTypedDictNotRequiredAccess]
                tuner=bridge_tuner,
                nonsticky_to_sticky_poll_ratio=config["nonsticky_to_sticky_poll_ratio"],  # type: ignore[reportTypedDictNotRequiredAccess]
                # We have to disable remote activities if a user asks _or_ if we
                # are not running an activity worker at all. Otherwise shutdown
                # will not proceed properly.
                no_remote_activities=config.get("no_remote_activities")
                or not config.get("activities"),
                task_types=temporalio.bridge.worker.WorkerTaskTypes(
                    enable_workflows=self._workflow_worker is not None,
                    enable_local_activities=self._activity_worker is not None
                    and self._workflow_worker is not None,
                    enable_remote_activities=self._activity_worker is not None
                    and not config.get("no_remote_activities"),
                    enable_nexus=self._nexus_worker is not None,
                ),
                sticky_queue_schedule_to_start_timeout_millis=int(
                    1000
                    * config["sticky_queue_schedule_to_start_timeout"].total_seconds()  # type: ignore[reportTypedDictNotRequiredAccess]
                ),
                max_heartbeat_throttle_interval_millis=int(
                    1000 * config["max_heartbeat_throttle_interval"].total_seconds()  # type: ignore[reportTypedDictNotRequiredAccess]
                ),
                default_heartbeat_throttle_interval_millis=int(
                    1000 * config["default_heartbeat_throttle_interval"].total_seconds()  # type: ignore[reportTypedDictNotRequiredAccess]
                ),
                max_activities_per_second=config.get("max_activities_per_second"),
                max_task_queue_activities_per_second=config[
                    "max_task_queue_activities_per_second"
                ],  # type: ignore[reportTypedDictNotRequiredAccess]
                max_eager_activity_reservations_per_workflow_task=config[
                    "max_eager_activity_reservations_per_workflow_task"
                ],  # type: ignore[reportTypedDictNotRequiredAccess]
                graceful_shutdown_period_millis=int(
                    1000 * config["graceful_shutdown_timeout"].total_seconds()  # type: ignore[reportTypedDictNotRequiredAccess]
                ),
                # Need to tell core whether we want to consider all
                # non-determinism exceptions as workflow fail, and whether we do
                # per workflow type
                nondeterminism_as_workflow_fail=self._workflow_worker is not None
                and self._workflow_worker.nondeterminism_as_workflow_fail(),
                nondeterminism_as_workflow_fail_for_types=(
                    self._workflow_worker.nondeterminism_as_workflow_fail_for_types()
                    if self._workflow_worker
                    else set()
                ),
                versioning_strategy=versioning_strategy,
                workflow_task_poller_behavior=workflow_task_poller_behavior._to_bridge(),
                activity_task_poller_behavior=activity_task_poller_behavior._to_bridge(),
                nexus_task_poller_behavior=config[
                    "nexus_task_poller_behavior"
                ]._to_bridge(),  # type: ignore[reportTypedDictNotRequiredAccess,reportOptionalMemberAccess]
                plugins=deduped_plugin_names,
                storage_drivers=deduped_storage_driver_types,
                disable_payload_error_limit=config.get(
                    "disable_payload_error_limit", False
                ),
            ),
        )

    def config(self, *, active_config: bool = False) -> WorkerConfig:
        """Config, as a dictionary, used to create this worker.

        Args:
            active_config: If true, return the modified configuration in use rather than the initial one
                provided to the worker.

        Returns:
            Configuration, shallow-copied.
        """
        config = self._config.copy() if active_config else self._initial_config.copy()
        config["activities"] = list(config.get("activities", []))
        config["workflows"] = list(config.get("workflows", []))
        return config

    @property
    def task_queue(self) -> str:
        """Task queue this worker is on."""
        return self._config["task_queue"]  # type: ignore[reportTypedDictNotRequiredAccess]

    @property
    def client(self) -> temporalio.client.Client:
        """Client currently set on the worker."""
        return self._config["client"]  # type: ignore[reportTypedDictNotRequiredAccess]

    @client.setter
    def client(self, value: temporalio.client.Client) -> None:
        """Update the client associated with the worker.

        Changing the client will make sure the worker starts using it for the
        next calls it makes. However, outstanding client calls will still
        complete with the existing client. The new client cannot be "lazy" and
        must be using the same runtime as the current client.
        """
        bridge_client = _extract_bridge_client_for_worker(value)
        if self._runtime is not bridge_client.config.runtime:
            raise ValueError(
                "New client is not on the same runtime as the existing client"
            )
        assert bridge_client._bridge_client
        self._bridge_worker.replace_client(bridge_client._bridge_client)
        self._config["client"] = value

        # Update the activity worker's client reference if activities are configured
        if self._activity_worker:
            self._activity_worker._client = value

        # Update the nexus worker's client reference if nexus services are configured
        if self._nexus_worker:
            self._nexus_worker._client = value

    def _set_default_workflow_logic_flag(
        self, flag: _WorkflowLogicFlag, *, enabled: bool
    ) -> None:
        if self._started:
            raise RuntimeError(
                "Cannot set default workflow logic flags after the worker has started"
            )
        if not self._workflow_worker:
            raise RuntimeError("Cannot set workflow logic flags without workflows")
        self._workflow_worker._set_default_workflow_logic_flag(flag, enabled=enabled)

    @property
    def is_running(self) -> bool:
        """Whether the worker is running.

        This is only ``True`` if the worker has been started and not yet
        shut down.
        """
        return self._started and not self.is_shutdown

    @property
    def is_shutdown(self) -> bool:
        """Whether the worker has run and shut down.

        This is only ``True`` if the worker was once started and then shutdown.
        This is not necessarily ``True`` after :py:meth:`shutdown` is first
        called because the shutdown process can take a bit.
        """
        return self._shutdown_complete_event.is_set()

    async def run(self) -> None:
        """Run the worker and wait on it to be shut down.

        This will not return until shutdown is complete. This means that
        activities have all completed after being told to cancel after the
        graceful timeout period.

        This method will raise if there is a worker fatal error. While
        :py:meth:`shutdown` does not need to be invoked in this case, it is
        harmless to do so. Otherwise, to shut down this worker, invoke
        :py:meth:`shutdown`.

        Technically this worker can be shutdown by issuing a cancel to this
        async function assuming that it is currently running. A cancel could
        also cancel the shutdown process. Therefore users are encouraged to use
        explicit shutdown instead.
        """

        def make_lambda(plugin: Plugin, next: Callable[[Worker], Awaitable[None]]):
            return lambda w: plugin.run_worker(w, next)

        next_function = lambda w: w._run()
        for plugin in reversed(self._plugins):
            next_function = make_lambda(plugin, next_function)

        await next_function(self)

    async def _run(self):
        # Eagerly validate which will do a namespace check in Core.
        await self._bridge_worker.validate()

        if self._started:
            raise RuntimeError("Already started")
        self._started = True

        # Create a task that raises when a shutdown is requested
        async def raise_on_shutdown():
            try:
                await self._shutdown_event.wait()
                raise _ShutdownRequested()
            except asyncio.CancelledError:
                pass

        tasks: dict[
            None | _ActivityWorker | _WorkflowWorker | _NexusWorker, asyncio.Task
        ] = {None: asyncio.create_task(raise_on_shutdown())}
        # Create tasks for workers
        if self._activity_worker:
            tasks[self._activity_worker] = asyncio.create_task(
                self._activity_worker.run()
            )
        if self._workflow_worker:
            tasks[self._workflow_worker] = asyncio.create_task(
                self._workflow_worker.run()
            )
        if self._nexus_worker:
            tasks[self._nexus_worker] = asyncio.create_task(self._nexus_worker.run())

        # Wait for either worker or shutdown requested
        wait_task = asyncio.wait(tasks.values(), return_when=asyncio.FIRST_EXCEPTION)
        try:
            await asyncio.shield(wait_task)

            # If any of the worker tasks failed, re-raise that as the exception
            exception = next(
                (t.exception() for w, t in tasks.items() if w and t.done()), None
            )
            if exception:
                logger.error("Worker failed, shutting down", exc_info=exception)
                if self._config["on_fatal_error"]:  # type: ignore[reportTypedDictNotRequiredAccess]
                    try:
                        await self._config["on_fatal_error"](exception)  # type: ignore[reportTypedDictNotRequiredAccess]
                    except:
                        logger.warning("Fatal error handler failed")

        except asyncio.CancelledError as user_cancel_err:
            # Represents user literally calling cancel
            logger.info("Worker cancelled, shutting down")
            exception = user_cancel_err

        # Cancel the shutdown task (safe if already done)
        tasks[None].cancel()
        graceful_timeout = self._config["graceful_shutdown_timeout"]  # type: ignore[reportTypedDictNotRequiredAccess]
        logger.info(
            f"Beginning worker shutdown, will wait {graceful_timeout} before cancelling activities"
        )

        # Initiate core worker shutdown
        self._bridge_worker.initiate_shutdown()

        # If any worker task had an exception, replace that task with a queue drain
        for worker, task in tasks.items():
            if worker and task.done() and task.exception():
                tasks[worker] = asyncio.create_task(worker.drain_poll_queue())

        # Notify shutdown occurring
        if self._activity_worker:
            self._activity_worker.notify_shutdown()
        if self._workflow_worker:
            self._workflow_worker.notify_shutdown()
        if self._nexus_worker:
            self._nexus_worker.notify_shutdown()

        # Wait for all tasks to complete (i.e. for poller loops to stop)
        await asyncio.wait(tasks.values())
        # Sometimes both workers throw an exception and since we only take the
        # first, Python may complain with "Task exception was never retrieved"
        # if we don't get the others. Therefore we call cancel on each task
        # which suppresses this.
        for task in tasks.values():
            task.cancel()

        # Let all activity / nexus operations completions finish. We cannot guarantee that
        # because poll shutdown completed (which means activities/operations completed)
        # that they got flushed to the server.
        if self._activity_worker:
            await self._activity_worker.wait_all_completed()
        if self._nexus_worker:
            await self._nexus_worker.wait_all_completed()

        # Do final shutdown
        try:
            await self._bridge_worker.finalize_shutdown()
        except:
            # Ignore errors here that can arise in some tests where the bridge
            # worker still has a reference
            pass

        # Mark as shutdown complete and re-raise exception if present
        self._shutdown_complete_event.set()
        if exception:
            raise exception

    async def shutdown(self) -> None:
        """Initiate a worker shutdown and wait until complete.

        This can be called before the worker has even started and is safe for
        repeated invocations. It simply sets a marker informing the worker to
        shut down as it runs.

        This will not return until the worker has completed shutting down.
        """
        self._shutdown_event.set()
        await self._shutdown_complete_event.wait()

    async def __aenter__(self) -> Worker:
        """Start the worker and return self for use by ``async with``.

        This is a wrapper around :py:meth:`run`. Please review that method.

        This takes a similar approach to :py:func:`asyncio.timeout` in that it
        will cancel the current task if there is a fatal worker error and raise
        that error out of the context manager. However, if the inner async code
        swallows/wraps the :py:class:`asyncio.CancelledError`, the exiting
        portion of the context manager will not raise the fatal worker error.
        """
        if self._async_context_inner_task:
            raise RuntimeError("Already started")
        self._async_context_inner_task = asyncio.current_task()
        if not self._async_context_inner_task:
            raise RuntimeError("Can only use async with inside a task")

        # Start a task that runs and if there's an error, cancels the current
        # task and re-raises the error
        async def run():
            try:
                await self.run()
            except BaseException as err:
                self._async_context_run_exception = err
                self._async_context_inner_task.cancel()  # type: ignore[union-attr]

        self._async_context_run_task = asyncio.create_task(run())
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, *args: Any) -> None:
        """Same as :py:meth:`shutdown` for use by ``async with``.

        Note, this will raise the worker fatal error if one occurred and the
        inner task cancellation was not inadvertently swallowed/wrapped.
        """
        # Wait for shutdown then run complete
        if not self._async_context_run_task:
            raise RuntimeError("Never started")
        await self.shutdown()
        # Cancel our run task
        self._async_context_run_task.cancel()
        # Only re-raise our exception if present and exc_type is cancel
        if exc_type is asyncio.CancelledError and self._async_context_run_exception:
            raise self._async_context_run_exception


class WorkerConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:class:`Worker`."""

    client: temporalio.client.Client
    task_queue: str
    activities: Sequence[Callable]
    nexus_service_handlers: Sequence[Any]
    workflows: Sequence[type]
    activity_executor: concurrent.futures.Executor | None
    workflow_task_executor: concurrent.futures.ThreadPoolExecutor | None
    nexus_task_executor: concurrent.futures.ThreadPoolExecutor | None
    workflow_runner: WorkflowRunner
    unsandboxed_workflow_runner: WorkflowRunner
    plugins: Sequence[Plugin]
    interceptors: Sequence[Interceptor]
    build_id: str | None
    identity: str | None
    max_cached_workflows: int
    max_concurrent_workflow_tasks: int | None
    max_concurrent_activities: int | None
    max_concurrent_local_activities: int | None
    max_concurrent_nexus_tasks: int | None
    tuner: WorkerTuner | None
    max_concurrent_workflow_task_polls: int | None
    nonsticky_to_sticky_poll_ratio: float
    max_concurrent_activity_task_polls: int | None
    no_remote_activities: bool
    sticky_queue_schedule_to_start_timeout: timedelta
    max_heartbeat_throttle_interval: timedelta
    default_heartbeat_throttle_interval: timedelta
    max_activities_per_second: float | None
    max_task_queue_activities_per_second: float | None
    max_eager_activity_reservations_per_workflow_task: int
    graceful_shutdown_timeout: timedelta
    workflow_failure_exception_types: Sequence[type[BaseException]]
    shared_state_manager: SharedStateManager | None
    debug_mode: bool
    disable_eager_activity_execution: bool
    on_fatal_error: Callable[[BaseException], Awaitable[None]] | None
    use_worker_versioning: bool
    disable_safe_workflow_eviction: bool
    deployment_config: WorkerDeploymentConfig | None
    patch_activation_callback: Callable[[PatchActivationInput], bool] | None
    workflow_task_poller_behavior: PollerBehavior
    activity_task_poller_behavior: PollerBehavior
    nexus_task_poller_behavior: PollerBehavior
    disable_payload_error_limit: bool
    max_workflow_task_external_storage_concurrency: int


def _warn_if_activity_executor_max_workers_is_inconsistent(
    config: WorkerConfig,
) -> None:
    activity_executor = config.get("activity_executor")
    max_workers = getattr(activity_executor, "_max_workers", None)
    concurrent_activities = config.get("max_concurrent_activities")
    tuner = config.get("tuner")
    if tuner and tuner._get_activities_max():
        concurrent_activities = tuner._get_activities_max()
    if isinstance(max_workers, int) and max_workers < (concurrent_activities or 0):
        warnings.warn(
            f"Worker max_concurrent_activities is {concurrent_activities} "
            + f"but activity_executor's max_workers is only {max_workers}",
            stacklevel=3,
        )


def _warn_if_nexus_task_executor_max_workers_is_inconsistent(
    config: WorkerConfig,
) -> None:
    nexus_task_executor = config.get("nexus_task_executor")
    max_workers = getattr(nexus_task_executor, "_max_workers", None)
    concurrent_nexus_tasks = config.get("max_concurrent_nexus_tasks")
    tuner = config.get("tuner")
    if tuner and tuner._get_nexus_tasks_max():
        concurrent_nexus_tasks = tuner._get_nexus_tasks_max()
    if isinstance(max_workers, int) and max_workers < (concurrent_nexus_tasks or 0):
        warnings.warn(
            f"Worker max_concurrent_nexus_tasks is {concurrent_nexus_tasks} "
            + f"but nexus_task_executor's max_workers is only {max_workers}",
            stacklevel=3,
        )


@dataclass
class WorkerDeploymentConfig:
    """Options for configuring the Worker Versioning feature."""

    version: WorkerDeploymentVersion
    use_worker_versioning: bool
    default_versioning_behavior: VersioningBehavior = VersioningBehavior.UNSPECIFIED

    def _to_bridge_worker_deployment_options(
        self,
    ) -> temporalio.bridge.worker.WorkerDeploymentOptions:
        return temporalio.bridge.worker.WorkerDeploymentOptions(
            version=temporalio.bridge.worker.WorkerDeploymentVersion(
                deployment_name=self.version.deployment_name,
                build_id=self.version.build_id,
            ),
            use_worker_versioning=self.use_worker_versioning,
            default_versioning_behavior=self.default_versioning_behavior.value,
        )


_default_build_id: str | None = None


def load_default_build_id(*, memoize: bool = True) -> str:
    """Load the default worker build ID.

    The worker build ID is a unique hash representing the entire set of code
    including Temporal code and external code. The default here is currently
    implemented by walking loaded modules and hashing their bytecode into a
    common hash.

    Args:
        memoize: If true, the default, this will cache to a global variable to
            keep from having to run again on successive calls.

    Returns:
        Unique identifier representing the set of running code.
    """
    # Memoize
    global _default_build_id
    if memoize and _default_build_id:
        return _default_build_id

    # The goal is to get a hash representing the set of runtime code, both
    # Temporal's and the user's. After all options were explored, we have
    # decided to default to hashing all bytecode of imported modules. We accept
    # that this has the following limitations:
    #
    # * Dynamic imports later on can affect this value
    # * Dynamic imports based on env var, platform, etc can affect this value
    # * Using the loader's get_code seems to use get_data which does a disk read
    # * Using the loader's get_code in rare cases can cause a compile()

    got_temporal_code = False
    m = hashlib.md5(usedforsecurity=False)
    for mod_name in sorted(sys.modules):
        # Try to read code
        code = _get_module_code(mod_name)
        if not code:
            continue
        if mod_name == "temporalio":
            got_temporal_code = True
        # Add to MD5 digest
        m.update(code)
    # If we didn't even get the temporalio module from this approach, this
    # approach is flawed and we prefer to return an error forcing user to
    # explicitly set the worker binary ID instead of silently using a value that
    # may never change
    if not got_temporal_code:
        raise RuntimeError(
            "Cannot get default unique worker binary ID, the value should be explicitly set"
        )
    # Return the hex digest
    digest = m.hexdigest()
    if memoize:
        _default_build_id = digest
    return digest


def _get_module_code(mod_name: str) -> bytes | None:
    # First try the module's loader and if that fails, try __cached__ file
    try:
        loader: Any = sys.modules[mod_name].__loader__
        code = loader.get_code(mod_name).co_code
        if code:
            return code
    except Exception:
        pass
    try:
        # Technically we could read smaller chunks per file here and update the
        # hash, but the benefit is negligible especially since many non-built-in
        # modules will use get_code above
        with open(sys.modules[mod_name].__cached__, "rb") as f:
            return f.read()
    except Exception:
        pass
    return None


def _extract_bridge_client_for_worker(
    client: temporalio.client.Client,
) -> temporalio.service._BridgeServiceClient:
    # Extract the bridge service client. We try the service on the client first,
    # then we support a worker_service_client on the client's service to return
    # underlying service client we can use.
    bridge_client: temporalio.service._BridgeServiceClient
    if isinstance(client.service_client, temporalio.service._BridgeServiceClient):
        bridge_client = client.service_client
    elif hasattr(client.service_client, "worker_service_client"):
        bridge_client = client.service_client.worker_service_client
        if not isinstance(bridge_client, temporalio.service._BridgeServiceClient):
            raise TypeError(  # type: ignore[reportUnreachable]
                "Client's worker_service_client cannot be used for a worker"
            )
    else:
        raise TypeError(
            "Client cannot be used for a worker. "
            + "Use the original client's service or set worker_service_client on the wrapped service with the original service client."
        )

    # We need an already connected client
    # TODO(cretz): How to connect to client inside Worker constructor here? In
    # the meantime, we disallow lazy clients from being used for workers. We
    # could check whether the connected client is present which means
    # lazy-but-already-connected clients would work, but that is confusing
    # to users that the client only works if they already made a call on it.
    if bridge_client.config.lazy:
        raise RuntimeError("Lazy clients cannot be used for workers")
    return bridge_client


class _ShutdownRequested(RuntimeError):
    pass
