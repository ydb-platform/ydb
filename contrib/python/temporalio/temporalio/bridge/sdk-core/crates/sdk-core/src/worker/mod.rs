mod activities;
pub(crate) mod client;
pub(crate) mod heartbeat;
mod nexus;
mod slot_provider;
pub(crate) mod tuner;
mod workflow;

/// The failure `type` set on the task failures workers synthesize when an outbound payload exceeds
/// the error limit, shared so every conversion site reports the same identifier.
pub(crate) const PAYLOADS_TOO_LARGE_FAILURE_TYPE: &str = "PayloadsTooLarge";

use temporalio_client::{Connection, PayloadErrorLimits, worker::NamespaceDescriptionSource};
use temporalio_common::{
    protos::{
        coresdk::{
            ActivitySlotInfo, LocalActivitySlotInfo, NamespaceInfo, NexusSlotInfo,
            WorkflowSlotInfo, activity_result::ActivityExecutionResult, namespace_info,
        },
        temporal::api::{
            enums::v1::VersioningBehavior,
            worker::v1::{PluginInfo, StorageDriverInfo},
        },
    },
    telemetry::TelemetryInstance,
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion},
};
pub use tuner::{
    FixedSizeSlotSupplier, ResourceBasedSlotsOptions, ResourceBasedSlotsOptionsBuilder,
    ResourceBasedTuner, ResourceBasedTunerConfig, ResourceController, ResourceSlotOptions,
    SlotSupplierOptions, TunerBuilder, TunerHolder, TunerHolderOptions,
};
// Re-export the generated builder (it's in the tuner module)
pub use tuner::TunerHolderOptionsBuilder;
pub(crate) use tuner::{RealSysInfo, SystemResourceInfo};

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};
pub(crate) use wft_poller::WFTPollerShared;

#[allow(unreachable_pub)] // re-exported in test_help::integ_helpers
pub use workflow::LEGACY_QUERY_ID;

use crate::{
    ActivityHeartbeat,
    abstractions::{MeteredPermitDealer, PermitDealerContextData, dbg_panic},
    pollers::{ActivityTaskOptions, BoxedActPoller, BoxedNexusPoller, LongPollBuffer},
    protosext::validate_activity_completion,
    telemetry::metrics::{
        MetricsContext, WorkerHeartbeatMetrics, activity_poller, activity_worker_type,
        local_activity_worker_type, nexus_poller, nexus_worker_type, workflow_worker_type,
    },
    worker::{
        activities::{LACompleteAction, LocalActivityManager, NextPendingLAAction},
        client::WorkerClient,
        heartbeat::SharedNamespaceWorker,
        nexus::NexusManager,
        workflow::{
            LAReqSink, LocalResolution, WorkflowBasics, Workflows, wft_poller,
            wft_poller::make_wft_poller,
        },
    },
};
use activities::WorkerActivityTasks;
use anyhow::bail;
use crossbeam_utils::atomic::AtomicCell;
use futures_util::{StreamExt, stream};
use gethostname::gethostname;
use parking_lot::{Mutex, RwLock};
use slot_provider::SlotProvider;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    convert::TryInto,
    future,
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_client::worker::{
    CancelActivityCallback, ClientWorker, HeartbeatCallback, HeartbeatSuccessCallback,
    SharedNamespaceWorkerTrait, Slot as SlotTrait,
};
#[cfg(test)]
use temporalio_common::protos::temporal::api::namespace::v1::NamespaceInfo as ApiNamespaceInfo;
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::{
            ActivityTaskCompletion,
            activity_task::ActivityTask,
            nexus::{NexusTask, NexusTaskCompletion},
            workflow_activation::{WorkflowActivation, remove_from_cache::EvictionReason},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            deployment,
            enums::v1::{TaskQueueKind, TaskQueueType, WorkerStatus},
            namespace::v1::namespace_info::Capabilities as ApiNamespaceCapabilities,
            taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
            worker::v1::{
                EnvironmentInfo, WorkerHeartbeat, WorkerHostInfo, WorkerPollerInfo, WorkerSlotsInfo,
            },
            workflowservice::v1::DescribeNamespaceResponse,
        },
    },
    telemetry::metrics::TemporalMeter,
    worker::WorkerTaskTypes,
};
use tokio::sync::{mpsc::unbounded_channel, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::Subscriber;
use uuid::Uuid;
#[cfg(any(feature = "test-utilities", test))]
use {
    crate::{
        pollers::{BoxedPoller, MockPermittedPollBuffer},
        protosext::ValidPollWFTQResponse,
    },
    futures_util::stream::BoxStream,
    temporalio_common::protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
    },
};

/// Defines per-worker configuration options
#[derive(Clone, bon::Builder)]
#[builder(on(String, into), state_mod(vis = "pub"), finish_fn(vis = "", name = build_internal))]
#[non_exhaustive]
pub struct WorkerConfig {
    /// The Temporal service namespace this worker is bound to
    pub namespace: String,
    /// What task queue will this worker poll from? This task queue name will be used for both
    /// workflow and activity polling.
    pub task_queue: String,
    /// A human-readable string that can identify this worker. Using something like sdk version
    /// and host name is a good default. If set, overrides the identity set (if any) on the client
    /// used by this worker.
    pub client_identity_override: Option<String>,
    /// If set nonzero, workflows will be cached and sticky task queues will be used, meaning that
    /// history updates are applied incrementally to suspended instances of workflow execution.
    /// Workflows are evicted according to a least-recently-used policy once the cache maximum is
    /// reached. Workflows may also be explicitly evicted at any time, or as a result of errors
    /// or failures.
    #[builder(default = 0)]
    pub max_cached_workflows: usize,
    /// Set a [crate::WorkerTuner] for this worker. Either this or at least one of the
    /// `max_outstanding_*` fields must be set.
    pub tuner: Option<Arc<dyn WorkerTuner + Send + Sync>>,
    /// Maximum number of concurrent poll workflow task requests we will perform at a time on this
    /// worker's task queue. See also [WorkerConfig::nonsticky_to_sticky_poll_ratio].
    /// If using SimpleMaximum, Must be at least 2 when `max_cached_workflows` > 0, or is an error.
    ///
    /// When left unset (`None`), the effective behavior is resolved at worker start: pollers are
    /// automatically enrolled into poller autoscaling when the namespace advertises the
    /// `poller_autoscaling_auto_enroll` capability, otherwise the default
    /// [PollerBehavior::SimpleMaximum] is used.
    pub workflow_task_poller_behavior: Option<PollerBehavior>,
    /// Only applies when using [PollerBehavior::SimpleMaximum]
    ///
    /// (max workflow task polls * this number) = the number of max pollers that will be allowed for
    /// the nonsticky queue when sticky tasks are enabled. If both defaults are used, the sticky
    /// queue will allow 4 max pollers while the nonsticky queue will allow one. The minimum for
    /// either poller is 1, so if the maximum allowed is 1 and sticky queues are enabled, there will
    /// be 2 concurrent polls.
    #[builder(default = 0.2)]
    pub nonsticky_to_sticky_poll_ratio: f32,
    /// Maximum number of concurrent poll activity task requests we will perform at a time on this
    /// worker's task queue. See [WorkerConfig::workflow_task_poller_behavior] for the meaning of
    /// leaving this unset (`None`).
    pub activity_task_poller_behavior: Option<PollerBehavior>,
    /// Maximum number of concurrent poll nexus task requests we will perform at a time on this
    /// worker's task queue. See [WorkerConfig::workflow_task_poller_behavior] for the meaning of
    /// leaving this unset (`None`).
    pub nexus_task_poller_behavior: Option<PollerBehavior>,
    /// Specifies which task types this worker will poll for.
    ///
    /// Note: At least one task type must be specified or the worker will fail validation.
    pub task_types: WorkerTaskTypes,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue where it may be picked up by any worker.
    #[builder(default = Duration::from_secs(10))]
    pub sticky_queue_schedule_to_start_timeout: Duration,

    /// Longest interval for throttling activity heartbeats
    #[builder(default = Duration::from_secs(60))]
    pub max_heartbeat_throttle_interval: Duration,

    /// Default interval for throttling activity heartbeats in case
    /// `ActivityOptions.heartbeat_timeout` is unset.
    /// When the timeout *is* set in the `ActivityOptions`, throttling is set to
    /// `heartbeat_timeout * 0.8`.
    #[builder(default = Duration::from_secs(30))]
    pub default_heartbeat_throttle_interval: Duration,

    /// Sets the maximum number of activities per second the task queue will dispatch, controlled
    /// server-side. Note that this only takes effect upon an activity poll request. If multiple
    /// workers on the same queue have different values set, they will thrash with the last poller
    /// winning.
    ///
    /// Setting this to a nonzero value will also disable eager activity execution.
    pub max_task_queue_activities_per_second: Option<f64>,

    /// Limits the number of activities per second that this worker will process. The worker will
    /// not poll for new activities if by doing so it might receive and execute an activity which
    /// would cause it to exceed this limit. Negative, zero, or NaN values will cause building
    /// the options to fail.
    pub max_worker_activities_per_second: Option<f64>,

    /// Maximum number of activity slots that may be reserved for eager execution when completing
    /// a workflow task. The default is 3. Setting this to zero disables eager activity execution.
    #[builder(default = 3)]
    pub max_eager_activity_reservations_per_workflow_task: usize,

    /// If set false (default), shutdown will not finish until all pending evictions have been
    /// issued and replied to. If set true shutdown will be considered complete when the only
    /// remaining work is pending evictions.
    ///
    /// This flag is useful during tests to avoid needing to deal with lots of uninteresting
    /// evictions during shutdown. Alternatively, if a lang implementation finds it easy to clean
    /// up during shutdown, setting this true saves some back-and-forth.
    #[builder(default = false)]
    pub ignore_evicts_on_shutdown: bool,

    /// Maximum number of next page (or initial) history event listing requests we'll make
    /// concurrently. I don't this it's worth exposing this to users until we encounter a reason.
    #[builder(default = 5)]
    pub fetching_concurrency: usize,

    /// If set, core will issue cancels for all outstanding activities and nexus operations after
    /// shutdown has been initiated and this amount of time has elapsed.
    pub graceful_shutdown_period: Option<Duration>,

    /// The amount of time core will wait before timing out activities using its own local timers
    /// after one of them elapses. This is to avoid racing with server's own tracking of the
    /// timeout.
    #[builder(default = Duration::from_secs(5))]
    pub local_timeout_buffer_for_activities: Duration,

    /// Any error types listed here will cause any workflow being processed by this worker to fail,
    /// rather than simply failing the workflow task.
    #[builder(default)]
    pub workflow_failure_errors: HashSet<WorkflowErrorType>,

    /// Like [WorkerConfig::workflow_failure_errors], but specific to certain workflow types (the
    /// map key).
    #[builder(default)]
    pub workflow_types_to_failure_errors: HashMap<String, HashSet<WorkflowErrorType>>,

    /// The maximum allowed number of workflow tasks that will ever be given to this worker at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed. Must be at least 2 if
    /// `max_cached_workflows` is > 0, or is an error.
    ///
    /// Mutually exclusive with `tuner`
    #[builder(into)]
    pub max_outstanding_workflow_tasks: Option<usize>,
    /// The maximum number of activity tasks that will ever be given to this worker concurrently.
    ///
    /// Mutually exclusive with `tuner`
    #[builder(into)]
    pub max_outstanding_activities: Option<usize>,
    /// The maximum number of local activity tasks that will ever be given to this worker
    /// concurrently.
    ///
    /// Mutually exclusive with `tuner`
    #[builder(into)]
    pub max_outstanding_local_activities: Option<usize>,
    /// The maximum number of nexus tasks that will ever be given to this worker
    /// concurrently.
    ///
    /// Mutually exclusive with `tuner`
    #[builder(into)]
    pub max_outstanding_nexus_tasks: Option<usize>,

    /// A versioning strategy for this worker.
    pub versioning_strategy: WorkerVersioningStrategy,

    /// List of plugins used by lang.
    #[builder(default)]
    pub plugins: HashSet<PluginInfo>,

    /// Skips the single worker+client+namespace+task_queue check
    #[builder(default = false)]
    pub skip_client_worker_set_check: bool,

    /// List of storage drivers used by lang.
    #[builder(default)]
    pub storage_drivers: HashSet<StorageDriverInfo>,

    /// If set, the worker won't enforce server payload/memo error limits on outbound completions —
    /// oversized payloads are still warned about, and the server still rejects them. When unset, an
    /// oversized completion is proactively failed as a WFT/activity rather than sent.
    /// NOTE: Experimental
    #[builder(default = false)]
    pub disable_payload_error_limit: bool,
}

impl WorkerConfig {
    /// Returns true if the configuration specifies we should fail a workflow on a certain error
    /// type rather than failing the workflow task.
    pub fn should_fail_workflow(
        &self,
        workflow_type: &str,
        error_type: &WorkflowErrorType,
    ) -> bool {
        self.workflow_failure_errors.contains(error_type)
            || self
                .workflow_types_to_failure_errors
                .get(workflow_type)
                .map(|s| s.contains(error_type))
                .unwrap_or(false)
    }

    pub(crate) fn computed_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        let wdv = match self.versioning_strategy {
            WorkerVersioningStrategy::None { ref build_id } => WorkerDeploymentVersion {
                deployment_name: "".to_owned(),
                build_id: build_id.clone(),
            },
            WorkerVersioningStrategy::WorkerDeploymentBased(ref opts) => opts.version.clone(),
            WorkerVersioningStrategy::LegacyBuildIdBased { ref build_id } => {
                WorkerDeploymentVersion {
                    deployment_name: "".to_owned(),
                    build_id: build_id.clone(),
                }
            }
        };
        if wdv.is_empty() { None } else { Some(wdv) }
    }
}

impl<S: worker_config_builder::IsComplete> WorkerConfigBuilder<S> {
    /// Build and validate the worker configuration
    pub fn build(self) -> Result<WorkerConfig, String> {
        let config = self.build_internal();
        let task_types = &config.task_types;
        if task_types.is_empty() {
            return Err("At least one task type must be enabled in `task_types`".to_string());
        }
        if !task_types.enable_workflows && task_types.enable_local_activities {
            return Err(
                "`task_types` cannot enable local activities without workflows".to_string(),
            );
        }

        if let Some(b) = config.workflow_task_poller_behavior {
            b.validate()?;
        }
        if let Some(b) = config.activity_task_poller_behavior {
            b.validate()?;
        }
        if let Some(b) = config.nexus_task_poller_behavior {
            b.validate()?;
        }

        if let Some(ref x) = config.max_worker_activities_per_second
            && (!x.is_normal() || x.is_sign_negative())
        {
            return Err(
                "`max_worker_activities_per_second` must be positive and nonzero".to_string(),
            );
        }

        if matches!(config.max_outstanding_workflow_tasks, Some(v) if v == 0) {
            return Err("`max_outstanding_workflow_tasks` must be > 0".to_string());
        }
        if matches!(config.max_outstanding_activities, Some(v) if v == 0) {
            return Err("`max_outstanding_activities` must be > 0".to_string());
        }
        if matches!(config.max_outstanding_local_activities, Some(v) if v == 0) {
            return Err("`max_outstanding_local_activities` must be > 0".to_string());
        }
        if matches!(config.max_outstanding_nexus_tasks, Some(v) if v == 0) {
            return Err("`max_outstanding_nexus_tasks` must be > 0".to_string());
        }

        if config.max_cached_workflows > 0 {
            if let Some(max_wft) = config.max_outstanding_workflow_tasks
                && max_wft < 2
            {
                return Err(
                    "`max_cached_workflows` > 0 requires `max_outstanding_workflow_tasks` >= 2"
                        .to_string(),
                );
            }
            if matches!(config.workflow_task_poller_behavior, Some(PollerBehavior::SimpleMaximum(u)) if u < 2)
            {
                return Err("`max_cached_workflows` > 0 requires `workflow_task_poller_behavior` to be at least 2".to_string());
            }
        }

        if config.tuner.is_some()
            && (config.max_outstanding_workflow_tasks.is_some()
                || config.max_outstanding_activities.is_some()
                || config.max_outstanding_local_activities.is_some())
        {
            return Err("max_outstanding_* fields are mutually exclusive with `tuner`".to_string());
        }

        match &config.versioning_strategy {
            WorkerVersioningStrategy::None { .. } => {}
            WorkerVersioningStrategy::WorkerDeploymentBased(d) => {
                if d.use_worker_versioning
                    && (d.version.build_id.is_empty() || d.version.deployment_name.is_empty())
                {
                    return Err("WorkerDeploymentVersion must have a non-empty build_id and deployment_name when deployment-based versioning is enabled".to_string());
                }
                if !d.use_worker_versioning && d.default_versioning_behavior.is_some() {
                    return Err("default_versioning_behavior must be None when use_worker_versioning is false".to_string());
                }
            }
            WorkerVersioningStrategy::LegacyBuildIdBased { build_id } => {
                if build_id.is_empty() {
                    return Err(
                        "Legacy build id-based versioning must have a non-empty build_id"
                            .to_string(),
                    );
                }
            }
        }

        Ok(config)
    }
}

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,
    client: Arc<dyn WorkerClient>,
    /// Worker instance key, unique identifier for this worker
    worker_instance_key: Uuid,
    /// The task subsystems (workflows, activity task manager, nexus manager). Built lazily once
    /// namespace capabilities are known so effective poller behavior can be resolved.
    #[allow(clippy::type_complexity)]
    task_subsystems: LazyLock<TaskSubsystems, Box<dyn FnOnce() -> TaskSubsystems + Send>>,
    /// Manages local activities. None if workflow polling is disabled (local activities require workflows)
    local_act_mgr: Option<Arc<LocalActivityManager>>,
    /// Has shutdown been called?
    shutdown_token: CancellationToken,
    /// Will be called at the end of each activation completion
    #[allow(clippy::type_complexity)] // Sorry clippy, there's no simple way to re-use here.
    post_activate_hook: Option<Box<dyn Fn(&Self, PostActivateHookData<'_>) + Send + Sync>>,
    /// Set when non-local activities are complete and should stop being polled
    non_local_activities_complete: Arc<AtomicBool>,
    /// Set when local activities are complete and should stop being polled
    local_activities_complete: Arc<AtomicBool>,
    /// Used to track all permits have been released
    all_permits_tracker: tokio::sync::Mutex<AllPermitsTracker>,
    /// Used to track worker client
    client_worker_registrator: Arc<ClientWorkerRegistrator>,
    /// Status of the worker
    status: Arc<RwLock<WorkerStatus>>,
    /// Capabilities from the namespace description shared by workers on this connection.
    capabilities: Arc<NamespaceCapabilities>,
    /// Handle for the spawned ShutdownWorker RPC task, awaited during shutdown.
    shutdown_rpc_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Namespace capabilities discovered via `describe_namespace`.
pub struct NamespaceCapabilities {
    description: Arc<NamespaceDescriptionSource>,
}

impl Default for NamespaceCapabilities {
    fn default() -> Self {
        Self {
            description: Arc::new(NamespaceDescriptionSource::resolved(
                DescribeNamespaceResponse::default(),
            )),
        }
    }
}

impl NamespaceCapabilities {
    fn new(description: Arc<NamespaceDescriptionSource>) -> Self {
        Self { description }
    }

    #[cfg(test)]
    pub(crate) fn resolved(capabilities: ApiNamespaceCapabilities) -> Self {
        Self::new(Arc::new(NamespaceDescriptionSource::resolved(
            DescribeNamespaceResponse {
                namespace_info: Some(ApiNamespaceInfo {
                    capabilities: Some(capabilities),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )))
    }

    fn capabilities(&self) -> Option<&ApiNamespaceCapabilities> {
        self.description
            .get()
            .and_then(|description| description.namespace_info.as_ref())
            .and_then(|namespace_info| namespace_info.capabilities.as_ref())
    }

    /// Returns true if the server supports graceful poll cancellation on shutdown, so pollers
    /// can let in-flight polls complete rather than hard-killing them.
    pub fn graceful_poll_shutdown(&self) -> bool {
        self.capabilities()
            .is_some_and(|capabilities| capabilities.worker_poll_complete_on_shutdown)
    }

    /// Returns true if pollers may scale down on poll timeout even without an explicit scaling
    /// decision from the server.
    pub fn poller_autoscaling(&self) -> bool {
        self.capabilities()
            .is_some_and(|capabilities| capabilities.poller_autoscaling)
    }

    /// Returns true if the namespace opts workers into poller autoscaling by default. Poller types
    /// left at their default are automatically enrolled into autoscaling when this is set.
    pub fn poller_autoscaling_auto_enroll(&self) -> bool {
        self.capabilities()
            .is_some_and(|capabilities| capabilities.poller_autoscaling_auto_enroll)
    }

    /// Returns true if worker commands are supported in this namespace.
    pub fn worker_commands(&self) -> bool {
        self.capabilities()
            .is_some_and(|capabilities| capabilities.worker_commands)
    }
}

/// Resolve the effective poller behavior. When no behavior was configured (`None`), pollers are
/// automatically enrolled into autoscaling if the namespace advertises the
/// `poller_autoscaling_auto_enroll` capability; otherwise the default [PollerBehavior::SimpleMaximum]
/// is used. A configured behavior is always used as-is.
pub(crate) fn resolve_effective_behavior(
    configured: Option<PollerBehavior>,
    capabilities: &NamespaceCapabilities,
) -> PollerBehavior {
    match configured {
        Some(b) => b,
        None if capabilities.poller_autoscaling_auto_enroll() => PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 100,
            initial: 5,
        },
        None => PollerBehavior::SimpleMaximum(5),
    }
}

/// The task subsystems, constructed lazily after namespace capabilities are known so that the
/// effective poller behavior can be resolved once (see [resolve_effective_behavior]).
struct TaskSubsystems {
    workflows: Option<Workflows>,
    at_task_mgr: Option<WorkerActivityTasks>,
    nexus_mgr: Option<NexusManager>,
}

struct AllPermitsTracker {
    wft_permits: watch::Receiver<usize>,
    act_permits: watch::Receiver<usize>,
    la_permits: watch::Receiver<usize>,
}

impl AllPermitsTracker {
    async fn all_done(&mut self) {
        let _ = self.wft_permits.wait_for(|x| *x == 0).await;
        let _ = self.act_permits.wait_for(|x| *x == 0).await;
        let _ = self.la_permits.wait_for(|x| *x == 0).await;
    }
}

#[derive(Clone)]
pub(crate) struct WorkerTelemetry {
    temporal_metric_meter: Option<TemporalMeter>,
    trace_subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,
}

pub(crate) struct WorkerHeartbeatOptions {
    interval: Duration,
    environment_info: Option<Arc<EnvironmentInfo>>,
}

impl WorkerTelemetry {
    #[cfg(any(feature = "test-utilities", test))]
    pub(crate) fn from_meter(meter: TemporalMeter) -> Self {
        Self {
            temporal_metric_meter: Some(meter),
            trace_subscriber: None,
        }
    }
}

impl Worker {
    /// Creates a new [Worker] from a [WorkerClient] instance with real task pollers and optional
    /// telemetry.
    pub(crate) fn new(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        telem_instance: Option<&TelemetryInstance>,
        worker_heartbeat_interval: Option<Duration>,
        environment_info: Option<Arc<EnvironmentInfo>>,
    ) -> Result<Worker, anyhow::Error> {
        info!(task_queue=%config.task_queue, namespace=%config.namespace, "Initializing worker");

        let worker_telemetry = telem_instance.map(|telem| WorkerTelemetry {
            temporal_metric_meter: telem.get_temporal_metric_meter(),
            trace_subscriber: telem.trace_subscriber(),
        });

        let worker_heartbeat_options =
            worker_heartbeat_interval.map(|interval| WorkerHeartbeatOptions {
                interval,
                environment_info,
            });

        Self::new_with_pollers(
            config,
            sticky_queue_name,
            client,
            TaskPollers::Real,
            worker_telemetry,
            worker_heartbeat_options,
            false,
        )
    }

    /// Validate that the worker can properly connect to server, plus any other validation that
    /// needs to be done asynchronously. Lang SDKs should call this function once before calling
    /// any others.
    pub async fn validate(&self) -> Result<NamespaceInfo, WorkerValidationError> {
        match self
            .capabilities
            .description
            .resolve(|| self.client.describe_namespace())
            .await
        {
            Ok(info) => {
                let limits = info.namespace_info.as_ref().and_then(|api_namespace_info| {
                    api_namespace_info
                        .limits
                        .as_ref()
                        .map(|api_limits| namespace_info::Limits {
                            blob_size_limit_error: api_limits.blob_size_limit_error,
                            memo_size_limit_error: api_limits.memo_size_limit_error,
                        })
                });
                // Install the namespace error limits on the client (enforced on completions) unless
                // opted out; warn-level enforcement is always on, configured on the connection.
                if !self.config.disable_payload_error_limit
                    && let Some(limits) = limits.as_ref()
                {
                    self.client
                        .set_payload_error_limits(Some(PayloadErrorLimits {
                            blob: limits.blob_size_limit_error.max(0) as usize,
                            memo: limits.memo_size_limit_error.max(0) as usize,
                        }));
                }
                // Now that capabilities are known, eagerly build the pollers so effective poller
                // behavior is resolved during the normal check-in path.
                LazyLock::force(&self.task_subsystems);
                Ok(NamespaceInfo { limits })
            }
            Err(e) => Err(WorkerValidationError::NamespaceDescribeError {
                source: e,
                namespace: self.config.namespace.clone(),
            }),
        }
    }

    /// Replace client.
    ///
    /// For eager workflow purposes, this new client will now apply to future eager start requests
    /// and the older client will not. Note, if this registration fails, the worker heartbeat will
    /// also not be registered.
    ///
    /// For worker heartbeat, this will remove an existing shared worker if it is the last worker of
    /// the old client and create a new nexus worker if it's the first client of the namespace on
    /// the new client.
    pub fn replace_client(&self, mut new_connection: Connection) -> Result<(), anyhow::Error> {
        // Unregister worker from current client, register in new client at the end
        self.client
            .workers()
            .unregister_slot_provider(self.worker_instance_key)?;
        let client_worker = self
            .client
            .workers()
            .finalize_unregister(self.worker_instance_key)?;

        super::init_worker_client(
            &mut new_connection,
            self.config.client_identity_override.clone(),
        );

        self.client.replace_connection(new_connection);
        *self.client_worker_registrator.client.write() = self.client.clone();
        self.client
            .workers()
            .register_worker(client_worker, self.config.skip_client_worker_set_check)
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        let sticky_queue_name = if config.max_cached_workflows > 0 {
            Some(format!("sticky-{}", config.task_queue))
        } else {
            None
        };
        Self::new(
            config,
            sticky_queue_name,
            Arc::new(client),
            None,
            None,
            None,
        )
        .unwrap()
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        task_pollers: TaskPollers,
        worker_telemetry: Option<WorkerTelemetry>,
        worker_heartbeat_options: Option<WorkerHeartbeatOptions>,
        shared_namespace_worker: bool,
    ) -> Result<Worker, anyhow::Error> {
        let (metrics, meter) = if let Some(wt) = worker_telemetry.as_ref() {
            (
                MetricsContext::top_level_with_meter(
                    config.namespace.clone(),
                    config.task_queue.clone(),
                    wt.temporal_metric_meter.clone(),
                ),
                wt.temporal_metric_meter.clone(),
            )
        } else {
            (MetricsContext::no_op(), None)
        };

        let mut sys_info = None;
        let tuner = config.tuner.as_ref().cloned().unwrap_or_else(|| {
            let mut tuner_builder = TunerBuilder::from_config(&config);
            sys_info = tuner_builder.get_sys_info();
            Arc::new(tuner_builder.build())
        });

        metrics.worker_registered();
        let shutdown_token = CancellationToken::new();
        let slot_context_data = Arc::new(PermitDealerContextData {
            task_queue: config.task_queue.clone(),
            worker_identity: client.identity(),
            worker_deployment_version: config.computed_deployment_version(),
        });
        let wft_slots = MeteredPermitDealer::new(
            tuner.workflow_task_slot_supplier(),
            metrics.with_new_attrs([workflow_worker_type()]),
            if config.max_cached_workflows > 0 {
                // Since we always need to be able to poll the normal task queue as well as the
                // sticky queue, we need a value of at least 2 here.
                Some(std::cmp::max(2, config.max_cached_workflows))
            } else {
                None
            },
            slot_context_data.clone(),
            meter.clone(),
        );
        let wft_permits = wft_slots.get_extant_count_rcv();
        let act_slots = MeteredPermitDealer::new(
            tuner.activity_task_slot_supplier(),
            metrics.with_new_attrs([activity_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let act_permits = act_slots.get_extant_count_rcv();
        let (external_wft_tx, external_wft_rx) = unbounded_channel();

        let wf_last_suc_poll_time = Arc::new(AtomicCell::new(None));
        let wf_sticky_last_suc_poll_time = Arc::new(AtomicCell::new(None));
        let act_last_suc_poll_time = Arc::new(AtomicCell::new(None));
        let nexus_last_suc_poll_time = Arc::new(AtomicCell::new(None));
        let namespace_description = client
            .workers()
            .namespace_description_source(config.namespace.as_str());
        let capabilities = Arc::new(NamespaceCapabilities::new(namespace_description.clone()));

        let nexus_slots = MeteredPermitDealer::new(
            tuner.nexus_task_slot_supplier(),
            metrics.with_new_attrs([nexus_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let la_permit_dealer = MeteredPermitDealer::new(
            tuner.local_activity_slot_supplier(),
            metrics.with_new_attrs([local_activity_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let la_permits = la_permit_dealer.get_extant_count_rcv();

        let (local_act_mgr, la_sink, hb_rx) = if config.task_types.enable_local_activities {
            let (hb_tx, hb_rx) = unbounded_channel();
            let local_act_mgr = Arc::new(LocalActivityManager::new(
                config.namespace.clone(),
                la_permit_dealer.clone(),
                hb_tx,
                metrics.clone(),
            ));
            let la_sink = LAReqSink::new(local_act_mgr.clone());
            (Some(local_act_mgr), Some(la_sink), Some(hb_rx))
        } else {
            (None, None, None)
        };

        // Determine, before `task_pollers` is moved into the deferred build closure, whether we
        // will poll for non-local activities.
        let poll_on_non_local_activities = config.task_types.enable_remote_activities
            && match &task_pollers {
                TaskPollers::Real => true,
                #[cfg(any(feature = "test-utilities", test))]
                TaskPollers::Mocked { act_poller, .. } => act_poller.is_some(),
            };
        if !poll_on_non_local_activities && !shared_namespace_worker {
            info!("Activity polling is disabled for this worker");
        };

        let worker_instance_key = client.worker_instance_key();
        let worker_status = Arc::new(RwLock::new(WorkerStatus::Running));
        let sdk_name_and_ver = client.sdk_name_and_version();

        // Slot filled by the deferred build with the activity cancellation callback, so the
        // client-worker registrator can resolve it lazily (the activity task manager doesn't
        // exist until the pollers are built).
        let cancel_activity_slot: Arc<OnceLock<CancelActivityCallback>> = Arc::new(OnceLock::new());

        let deployment_options = match &config.versioning_strategy {
            WorkerVersioningStrategy::WorkerDeploymentBased(opts) => Some(opts.clone()),
            _ => None,
        };
        let provider = SlotProvider::new(
            config.namespace.clone(),
            config.task_queue.clone(),
            wft_slots.clone(),
            external_wft_tx,
            deployment_options,
        );

        let worker_heartbeat = worker_heartbeat_options.map(|heartbeat_options| {
            let hb_interval = heartbeat_options.interval;
            let heartbeat_sys_info =
                sys_info.unwrap_or_else(|| Arc::new(RealSysInfo::new(hb_interval)));
            let hb_metrics = HeartbeatMetrics {
                in_mem_metrics: metrics.in_memory_meter(),
                wft_slots: wft_slots.clone(),
                act_slots: act_slots.clone(),
                nexus_slots: nexus_slots.clone(),
                la_slots: la_permit_dealer,
                wf_last_suc_poll_time: wf_last_suc_poll_time.clone(),
                wf_sticky_last_suc_poll_time: wf_sticky_last_suc_poll_time.clone(),
                act_last_suc_poll_time: act_last_suc_poll_time.clone(),
                nexus_last_suc_poll_time: nexus_last_suc_poll_time.clone(),
                status: worker_status.clone(),
                sys_info: heartbeat_sys_info,
            };
            WorkerHeartbeatManager::new(
                config.clone(),
                worker_instance_key,
                hb_interval,
                worker_telemetry.clone(),
                hb_metrics,
                capabilities.clone(),
                heartbeat_options.environment_info,
            )
        });

        let client_worker_registrator = Arc::new(ClientWorkerRegistrator {
            worker_instance_key,
            slot_provider: provider,
            heartbeat_manager: worker_heartbeat,
            cancel_activity_slot: cancel_activity_slot.clone(),
            client: RwLock::new(client.clone()),
            namespace_description,
            shared_namespace_worker,
            task_types: config.task_types,
        });

        if !shared_namespace_worker {
            client.workers().register_worker(
                client_worker_registrator.clone(),
                config.skip_client_worker_set_check,
            )?;
        }

        let worker_config = config.clone();
        let worker_client = client.clone();
        let worker_shutdown_token = shutdown_token.clone();
        let worker_local_act_mgr = local_act_mgr.clone();
        let worker_capabilities = capabilities.clone();

        // Build the poller-dependent subsystems lazily. This closure runs once, after namespace
        // capabilities have been fetched (see `Worker::validate`), so the effective poller behavior
        // can be resolved with knowledge of those capabilities.
        let task_subsystems_builder: Box<dyn FnOnce() -> TaskSubsystems + Send> = Box::new(
            move || {
                let (wft_stream, act_poller, nexus_poller) = match task_pollers {
                    TaskPollers::Real => {
                        let wft_stream = if config.task_types.enable_workflows {
                            let stream = make_wft_poller(
                                &config,
                                &sticky_queue_name,
                                &client,
                                &metrics,
                                &shutdown_token,
                                &wft_slots,
                                wf_last_suc_poll_time.clone(),
                                wf_sticky_last_suc_poll_time.clone(),
                                capabilities.clone(),
                            )
                            .boxed();
                            let stream = if !client.is_mock() {
                                // Some replay tests combine a mock client with real pollers,
                                // and they don't need to use the external stream
                                stream::select(
                                    stream,
                                    UnboundedReceiverStream::new(external_wft_rx),
                                )
                                .left_stream()
                            } else {
                                stream.right_stream()
                            };
                            Some(stream)
                        } else {
                            None
                        };

                        let act_poll_buffer = if config.task_types.enable_remote_activities {
                            let act_metrics = metrics.with_new_attrs([activity_poller()]);
                            let ap = LongPollBuffer::new_activity_task(
                                client.clone(),
                                config.task_queue.clone(),
                                resolve_effective_behavior(
                                    config.activity_task_poller_behavior,
                                    &capabilities,
                                ),
                                act_slots.clone(),
                                shutdown_token.child_token(),
                                Some(move |np| act_metrics.record_num_pollers(np)),
                                ActivityTaskOptions {
                                    max_worker_acts_per_second: config
                                        .max_worker_activities_per_second,
                                    max_tps: config.max_task_queue_activities_per_second,
                                },
                                act_last_suc_poll_time.clone(),
                                capabilities.clone(),
                            );
                            Some(Box::from(ap) as BoxedActPoller)
                        } else {
                            None
                        };

                        let nexus_poll_buffer = if config.task_types.enable_nexus {
                            let np_metrics = metrics.with_new_attrs([nexus_poller()]);
                            Some(Box::new(LongPollBuffer::new_nexus_task(
                                client.clone(),
                                config.task_queue.clone(),
                                resolve_effective_behavior(
                                    config.nexus_task_poller_behavior,
                                    &capabilities,
                                ),
                                nexus_slots.clone(),
                                shutdown_token.child_token(),
                                Some(move |np| np_metrics.record_num_pollers(np)),
                                nexus_last_suc_poll_time,
                                capabilities,
                                shared_namespace_worker,
                            )) as BoxedNexusPoller)
                        } else {
                            None
                        };

                        #[cfg(any(feature = "test-utilities", test))]
                        let wft_stream = wft_stream.map(|s| s.left_stream());
                        (wft_stream, act_poll_buffer, nexus_poll_buffer)
                    }
                    #[cfg(any(feature = "test-utilities", test))]
                    TaskPollers::Mocked {
                        wft_stream,
                        act_poller,
                        nexus_poller,
                    } => {
                        let wft_stream = config
                            .task_types
                            .enable_workflows
                            .then_some(wft_stream)
                            .flatten();
                        let act_poller = config
                            .task_types
                            .enable_remote_activities
                            .then_some(act_poller)
                            .flatten();
                        let nexus_poller = config
                            .task_types
                            .enable_nexus
                            .then_some(nexus_poller)
                            .flatten();

                        let ap = act_poller.map(|ap| {
                            MockPermittedPollBuffer::new(Arc::new(act_slots.clone()), ap)
                        });
                        let np = nexus_poller.map(|np| {
                            MockPermittedPollBuffer::new(Arc::new(nexus_slots.clone()), np)
                        });
                        let wfs = wft_stream.map(|stream| {
                            let wft_semaphore = wft_slots.clone();
                            let wfs = stream.then(move |s| {
                                let wft_semaphore = wft_semaphore.clone();
                                async move {
                                    let permit = wft_semaphore.acquire_owned().await;
                                    s.map(|s| (s, permit))
                                }
                            });
                            wfs.right_stream()
                        });
                        (
                            wfs,
                            ap.map(|ap| Box::new(ap) as BoxedActPoller),
                            np.map(|np| Box::new(np) as BoxedNexusPoller),
                        )
                    }
                };

                if act_poller.is_some()
                    && !client
                        .capabilities()
                        .is_some_and(|caps| caps.activity_failure_include_heartbeat)
                {
                    warn!(
                        "Temporal Server 1.16.0 or newer is required to guarantee that the latest heartbeat details are preserved when an activity fails; the server did not advertise the activity_failure_include_heartbeat capability, so heartbeat details may be lost on failure"
                    );
                }
                let at_task_mgr = act_poller.map(|ap| {
                    WorkerActivityTasks::new(
                        act_slots.clone(),
                        ap,
                        client.clone(),
                        metrics.clone(),
                        config.max_heartbeat_throttle_interval,
                        config.default_heartbeat_throttle_interval,
                        config.graceful_shutdown_period,
                        config.local_timeout_buffer_for_activities,
                    )
                });

                if let Some(mgr) = &at_task_mgr {
                    let _ = cancel_activity_slot.set(mgr.cancel_activity_callback());
                }

                let nexus_mgr = nexus_poller.map(|poller| {
                    NexusManager::new(
                        poller,
                        metrics.clone(),
                        config.graceful_shutdown_period,
                        shutdown_token.child_token(),
                    )
                });

                let workflows = wft_stream.map(|stream| {
                    Workflows::new(
                        WorkflowBasics {
                            worker_config: Arc::new(config.clone()),
                            shutdown_token: shutdown_token.child_token(),
                            metrics,
                            server_capabilities: client.capabilities().unwrap_or_default(),
                            sdk_name: sdk_name_and_ver.0,
                            sdk_version: sdk_name_and_ver.1,
                            default_versioning_behavior: config
                                .versioning_strategy
                                .default_versioning_behavior(),
                        },
                        sticky_queue_name.map(|sq| StickyExecutionAttributes {
                            worker_task_queue: Some(TaskQueue {
                                name: sq,
                                kind: TaskQueueKind::Sticky as i32,
                                normal_name: config.task_queue,
                            }),
                            schedule_to_start_timeout: Some(
                                config
                                    .sticky_queue_schedule_to_start_timeout
                                    .try_into()
                                    .expect("timeout fits into proto"),
                            ),
                        }),
                        client,
                        wft_slots,
                        stream,
                        la_sink,
                        local_act_mgr,
                        hb_rx,
                        at_task_mgr.as_ref().and_then(|mgr| {
                            match config.max_task_queue_activities_per_second {
                                Some(persec) if persec > 0.0 => None,
                                _ => Some(mgr.get_handle_for_workflows()),
                            }
                        }),
                        worker_telemetry
                            .as_ref()
                            .and_then(|telem| telem.trace_subscriber.clone()),
                    )
                });
                TaskSubsystems {
                    workflows,
                    at_task_mgr,
                    nexus_mgr,
                }
            },
        );

        Ok(Self {
            worker_instance_key,
            client: worker_client,
            task_subsystems: LazyLock::new(task_subsystems_builder),
            local_act_mgr: worker_local_act_mgr,
            config: worker_config,
            shutdown_token: worker_shutdown_token,
            post_activate_hook: None,
            // Non-local activities are already complete if configured not to poll for them.
            non_local_activities_complete: Arc::new(AtomicBool::new(!poll_on_non_local_activities)),
            local_activities_complete: Default::default(),
            all_permits_tracker: tokio::sync::Mutex::new(AllPermitsTracker {
                wft_permits,
                act_permits,
                la_permits,
            }),
            client_worker_registrator,
            status: worker_status,
            capabilities: worker_capabilities,
            shutdown_rpc_handle: Mutex::new(None),
        })
    }

    fn mark_started(&self) {
        if self.shutdown_token.is_cancelled() {
            return;
        }
        if let Some(heartbeat_manager) = self.client_worker_registrator.heartbeat_manager.as_ref() {
            heartbeat_manager.mark_started();
        }
    }

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down this worker. [Worker::poll_workflow_activation] and [Worker::poll_activity_task] should
    /// be called until both return a `ShutDown` error to ensure that all outstanding work is
    /// complete. This means that the lang sdk will need to call
    /// [Worker::complete_workflow_activation] and [Worker::complete_activity_task] for those
    /// workflows & activities until they are done. At that point, the lang SDK can end the process,
    /// or drop the [Worker] instance via [Worker::finalize_shutdown], which will close the
    /// connection and free resources. If you have set [WorkerConfig::task_types] to exclude
    /// [WorkerTaskTypes::activity_only()], you may skip calling [Worker::poll_activity_task].
    ///
    /// Lang implementations should use [Worker::initiate_shutdown] followed by
    /// [Worker::finalize_shutdown].
    pub async fn shutdown(&self) {
        self.initiate_shutdown();

        // Ensure the ShutdownWorker RPC completes before waiting for polls to drain,
        // otherwise graceful poll shutdown deadlocks.
        let handle = self.shutdown_rpc_handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        // We need to wait for all local activities to finish so no more workflow task heartbeats
        // will be generated
        if let Some(la_mgr) = &self.local_act_mgr {
            la_mgr.wait_all_outstanding_tasks_finished().await;
        }
        // Wait for workflows to finish. If a caller reached shutdown without ever polling, this
        // deref builds the subsystems; the build closure sees the cancelled shutdown token and
        // signals them, so this still terminates promptly.
        if let Some(workflows) = self.task_subsystems.workflows.as_ref() {
            workflows
                .shutdown()
                .await
                .expect("Workflow processing terminates cleanly");
        }
        // Wait for activities to finish
        if let Some(acts) = self.task_subsystems.at_task_mgr.as_ref() {
            acts.shutdown().await;
        }
        // Wait for nexus tasks to finish
        if let Some(nexus) = self.task_subsystems.nexus_mgr.as_ref() {
            nexus.shutdown().await;
        }
        // Wait for all permits to be released, but don't totally hang real-world shutdown.
        tokio::select! {
            _ = async { self.all_permits_tracker.lock().await.all_done().await } => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                dbg_panic!("Waiting for all slot permits to release took too long!");
            }
        }
    }

    /// Completes shutdown and frees all resources. You should avoid simply dropping workers, as
    /// this does not allow async tasks to report any panics that may have occurred cleanly.
    ///
    /// This should be called only after [Worker::shutdown] has resolved and/or both polling
    /// functions have returned `ShutDown` errors.
    pub async fn finalize_shutdown(self) {
        self.shutdown().await;
        if let Some(atm) = self.task_subsystems.at_task_mgr.as_ref() {
            atm.shutdown().await;
        }
        // Only after worker is fully shutdown do we remove the heartbeat callback
        // from SharedNamespaceWorker, allowing for accurate worker shutdown
        // from Server POV
        let _res = self
            .client
            .workers()
            .finalize_unregister(self.worker_instance_key);
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Returns number of currently cached workflows
    pub async fn cached_workflows(&self) -> usize {
        match self.task_subsystems.workflows.as_ref() {
            Some(workflows) => workflows
                .get_state_info()
                .await
                .map(|r| r.cached_workflows)
                .unwrap_or_default(),
            None => 0,
        }
    }

    /// Returns number of currently outstanding workflow tasks
    #[cfg(test)]
    pub(crate) async fn outstanding_workflow_tasks(&self) -> usize {
        match self.task_subsystems.workflows.as_ref() {
            Some(workflows) => workflows
                .get_state_info()
                .await
                .map(|r| r.outstanding_wft)
                .unwrap_or_default(),
            None => 0,
        }
    }

    #[allow(unused)]
    pub(crate) fn available_wft_permits(&self) -> Option<usize> {
        self.task_subsystems
            .workflows
            .as_ref()
            .and_then(|w| w.available_wft_permits())
    }
    #[cfg(test)]
    pub(crate) fn unused_wft_permits(&self) -> Option<usize> {
        self.task_subsystems
            .workflows
            .as_ref()
            .and_then(|w| w.unused_wft_permits())
    }

    /// Ask the worker for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// Do not call poll concurrently. It handles polling the server concurrently internally.
    ///
    /// Local activities are returned first before polling the server if there are any.
    #[instrument(skip(self))]
    pub async fn poll_activity_task(&self) -> Result<ActivityTask, PollError> {
        self.mark_started();
        loop {
            match self.activity_poll().await.transpose() {
                Some(r) => break r,
                None => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }
    }

    async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollError> {
        let local_activities_complete = self.local_activities_complete.load(Ordering::Relaxed);
        let non_local_activities_complete =
            self.non_local_activities_complete.load(Ordering::Relaxed);
        if local_activities_complete && non_local_activities_complete {
            return Err(PollError::ShutDown);
        }
        let act_mgr_poll = async {
            if non_local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            if self.config.task_types.enable_remote_activities {
                if let Some(act_mgr) = self.task_subsystems.at_task_mgr.as_ref() {
                    let res = act_mgr.poll().await;
                    if let Err(err) = res.as_ref()
                        && matches!(err, PollError::ShutDown)
                    {
                        self.non_local_activities_complete
                            .store(true, Ordering::Relaxed);
                        return Ok(None);
                    };
                    res.map(Some)
                } else {
                    // We expect the local activity branch below to produce shutdown when appropriate if
                    // there are no activity pollers.
                    future::pending::<()>().await;
                    unreachable!()
                }
            } else {
                self.non_local_activities_complete
                    .store(true, Ordering::Relaxed);
                Ok(None)
            }
        };
        let local_activities_poll = async {
            if local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            if self.config.task_types.enable_local_activities {
                match &self.local_act_mgr {
                    Some(la_mgr) => match la_mgr.next_pending().await {
                        Some(NextPendingLAAction::Dispatch(r)) => Ok(Some(r)),
                        Some(NextPendingLAAction::Autocomplete(action)) => {
                            Ok(self.handle_la_complete_action(action))
                        }
                        None => {
                            if self.shutdown_token.is_cancelled() {
                                self.local_activities_complete
                                    .store(true, Ordering::Relaxed);
                            }
                            Ok(None)
                        }
                    },
                    None => {
                        self.local_activities_complete
                            .store(true, Ordering::Relaxed);
                        Ok(None)
                    }
                }
            } else {
                self.local_activities_complete
                    .store(true, Ordering::Relaxed);
                Ok(None)
            }
        };

        let r = tokio::select! {
            biased;

            r = local_activities_poll => r,
            r = act_mgr_poll => r,
        };
        // Since we consider network errors (at this level) fatal, we want to start shutdown if one
        // is encountered
        if matches!(r, Err(PollError::TonicError(_))) {
            self.initiate_shutdown();
        }
        r
    }

    /// Notify the Temporal service that an activity is still alive. Long running activities that
    /// take longer than `activity_heartbeat_timeout` to finish must call this function in order to
    /// report progress, otherwise the activity will timeout and a new attempt will be scheduled.
    ///
    /// The first heartbeat request will be sent immediately, subsequent rapid calls to this
    /// function will result in heartbeat requests being aggregated and the last one received during
    /// the aggregation period will be sent to the server, where that period is defined as half the
    /// heartbeat timeout.
    ///
    /// Unlike Java/Go SDKs we do not return cancellation status as part of heartbeat response and
    /// instead send it as a separate activity task to the lang, decoupling heartbeat and
    /// cancellation processing.
    ///
    /// Activities may receive cancellation requests independently from heartbeating. Note that
    /// this function does not block on the server call and returns immediately. Underlying
    /// validation errors are swallowed and logged, this has been agreed to be optimal behavior for
    /// the user as we don't want to break activity execution due to badly configured heartbeat
    /// options.
    pub fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        if let Some(at_mgr) = self.task_subsystems.at_task_mgr.as_ref() {
            let tt = TaskToken(details.task_token.clone());
            if let Err(e) = at_mgr.record_heartbeat(details) {
                warn!(task_token = %tt, details = ?e, "Activity heartbeat failed.");
            }
        }
    }

    /// Tell the worker that an activity has finished executing. May (and should) be freely called
    /// concurrently.
    #[instrument(skip(self, completion),
                 fields(task_token, status,
                        task_queue=%self.config.task_queue, workflow_id, run_id))]
    pub async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        let task_token = TaskToken(completion.task_token);
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };

        tracing::Span::current().record("task_token", task_token.to_string());
        tracing::Span::current().record("status", status.to_string());

        validate_activity_completion(&status)?;
        if task_token.is_local_activity_task() {
            let as_la_res: LocalActivityExecutionResult = status.try_into()?;
            self.complete_local_act(task_token, as_la_res);
            return Ok(());
        }

        if let Some(atm) = self.task_subsystems.at_task_mgr.as_ref() {
            atm.complete(task_token, status, &*self.client).await;
            Ok(())
        } else {
            Err(CompleteActivityError::ActivityNotEnabled)
        }
    }

    /// Ask the worker for some work, returning a [WorkflowActivation]. It is then the language
    /// SDK's responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// It is important to understand that all activations must be responded to. There can only
    /// be one outstanding activation for a particular run of a workflow at any time. If an
    /// activation is not responded to, it will cause that workflow to become stuck forever.
    ///
    /// See [WorkflowActivation] for more details on the expected behavior of lang w.r.t activation
    /// & job processing.
    ///
    /// Do not call poll concurrently. It handles polling the server concurrently internally.
    #[instrument(skip(self), fields(run_id, workflow_id, task_queue=%self.config.task_queue))]
    pub async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollError> {
        self.mark_started();
        match self.task_subsystems.workflows.as_ref() {
            Some(workflows) => {
                let r = workflows.next_workflow_activation().await;
                // In the event workflows are shutdown or erroring, begin shutdown of everything else. Once
                // they are shut down, tell the local activity manager that, so that it can know to cancel
                // any remaining outstanding LAs and shutdown.
                if let Err(ref e) = r {
                    // This is covering the situation where WFT pollers dying is the reason for shutdown
                    self.initiate_shutdown();
                    if matches!(e, PollError::ShutDown)
                        && let Some(la_mgr) = &self.local_act_mgr
                    {
                        la_mgr.workflows_have_shutdown();
                    }
                }
                r
            }
            None => Err(PollError::ShutDown),
        }
    }

    /// Tell the worker that a workflow activation has completed. May (and should) be freely called
    /// concurrently. The future may take some time to resolve, as fetching more events might be
    /// necessary for completion to... complete - thus SDK implementers should make sure they do
    /// not serialize completions.
    #[instrument(skip(self, completion),
        fields(completion=%&completion, run_id=%completion.run_id, workflow_id,
               task_queue=%self.config.task_queue))]
    pub async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        match self.task_subsystems.workflows.as_ref() {
            Some(workflows) => {
                workflows
                    .activation_completed(
                        completion,
                        false,
                        self.post_activate_hook
                            .as_ref()
                            .map(|h| |data: PostActivateHookData| h(self, data)),
                    )
                    .await?;
                Ok(())
            }
            None => Err(CompleteWfError::WorkflowNotEnabled),
        }
    }

    /// Ask the worker for some nexus related work. It is then the language SDK's
    /// responsibility to call the appropriate nexus operation handler code with the provided
    /// inputs. Blocks indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// All tasks must be responded to for shutdown to complete.
    ///
    /// Do not call poll concurrently. It handles polling the server concurrently internally.
    #[instrument(skip(self))]
    pub async fn poll_nexus_task(&self) -> Result<NexusTask, PollError> {
        self.mark_started();
        match self.task_subsystems.nexus_mgr.as_ref() {
            Some(mgr) => mgr.next_nexus_task().await,
            None => Err(PollError::ShutDown),
        }
    }

    /// Tell the worker that a nexus task has completed. May (and should) be freely called
    /// concurrently.
    #[instrument(
        skip(self, completion),
        fields(task_token, status, task_queue=%self.config.task_queue)
    )]
    pub async fn complete_nexus_task(
        &self,
        completion: NexusTaskCompletion,
    ) -> Result<(), CompleteNexusError> {
        let status = if let Some(s) = completion.status {
            s
        } else {
            return Err(CompleteNexusError::MalformedNexusCompletion {
                reason: "Nexus completion had empty status field".to_owned(),
            });
        };
        let tt = TaskToken(completion.task_token);
        tracing::Span::current().record("task_token", tt.to_string());
        tracing::Span::current().record("status", status.to_string());

        match self.task_subsystems.nexus_mgr.as_ref() {
            Some(mgr) => mgr.complete_task(tt, status, &*self.client).await,
            None => Err(CompleteNexusError::NexusNotEnabled),
        }
    }

    /// Request that a workflow be evicted by its run id. This will generate a workflow activation
    /// with the eviction job inside it to be eventually returned by
    /// [Worker::poll_workflow_activation]. If the workflow had any existing outstanding
    /// activations, such activations are invalidated and subsequent completions of them will do
    /// nothing and log a warning.
    pub fn request_workflow_eviction(&self, run_id: &str) {
        self.request_wf_eviction(
            run_id,
            "Eviction explicitly requested by lang",
            EvictionReason::LangRequested,
        );
    }

    /// Request a workflow eviction
    pub(crate) fn request_wf_eviction(
        &self,
        run_id: &str,
        message: impl Into<String>,
        reason: EvictionReason,
    ) {
        if let Some(workflows) = self.task_subsystems.workflows.as_ref() {
            workflows.request_eviction(run_id, message, reason);
        } else {
            dbg_panic!("trying to request wf eviction when workflows not enabled for this worker");
        }
    }

    /// Return this worker's config
    pub fn get_config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Return a clone of the current client connection backing this worker, if available.
    pub fn get_client_connection(&self) -> Option<Connection> {
        self.client.connection()
    }

    /// Returns the namespace capabilities discovered during worker initialization.
    ///
    /// Lang SDKs should call [Worker::validate] before reading these capabilities when worker
    /// heartbeating is disabled, since validation is then what resolves the namespace description.
    pub fn get_namespace_capabilities(&self) -> &NamespaceCapabilities {
        &self.capabilities
    }

    /// Initiate shutdown, including spawning the `ShutdownWorker` RPC so the server can complete
    /// in-flight polls. The RPC runs in a background task and is awaited in [Worker::shutdown].
    ///
    /// You can then wait on `shutdown` or [Worker::finalize_shutdown].
    pub fn initiate_shutdown(&self) {
        if !self.shutdown_token.is_cancelled() {
            info!(
                task_queue=%self.config.task_queue,
                namespace=%self.config.namespace,
                "Initiated shutdown",
            );
        }
        let already_initiated_shutdown = self.shutdown_token.is_cancelled();
        self.shutdown_token.cancel();
        {
            *self.status.write() = WorkerStatus::ShuttingDown;
        }
        // First, disable Eager Workflow Start
        if !self.client_worker_registrator.shared_namespace_worker {
            let _res = self
                .client
                .workers()
                .unregister_slot_provider(self.worker_instance_key);
        }

        // Push a BumpStream message to the workflow activation queue. This ensures that
        // any pending workflow activation polls will resolve, even if there are no other inputs.
        if let Some(workflows) = self.task_subsystems.workflows.as_ref() {
            workflows.bump_stream();
        }

        // Second, we want to stop polling of both activity and workflow tasks
        if let Some(atm) = self.task_subsystems.at_task_mgr.as_ref() {
            atm.initiate_shutdown();
        }
        // Let the manager know that shutdown has been initiated to try to unblock the local
        // activity poll in case this worker is an activity-only worker.
        if let Some(la_mgr) = &self.local_act_mgr {
            la_mgr.shutdown_initiated();

            // If workflows have never been polled, immediately tell the local activity manager
            // that workflows have shut down, so it can proceed with shutdown without waiting.
            // This is particularly important for activity-only workers.
            if self
                .task_subsystems
                .workflows
                .as_ref()
                .is_none_or(|w| !w.ever_polled())
            {
                la_mgr.workflows_have_shutdown();
            }
        }

        // Spawn the ShutdownWorker RPC so the server can complete in-flight polls.
        // The handle is stored and awaited in shutdown() to ensure completion.
        let mut guard = self.shutdown_rpc_handle.lock();
        if guard.is_some() || already_initiated_shutdown {
            return;
        }

        let client = self.client.clone();
        let sticky_name = self
            .task_subsystems
            .workflows
            .as_ref()
            .and_then(|wf| wf.get_sticky_queue_name())
            .unwrap_or_default();
        let task_queue = self.config.task_queue.clone();
        let mut task_queue_types = Vec::new();
        if self.config.task_types.enable_workflows {
            task_queue_types.push(TaskQueueType::Workflow);
        }
        if self.config.task_types.enable_remote_activities {
            task_queue_types.push(TaskQueueType::Activity);
        }
        if self.config.task_types.enable_nexus {
            task_queue_types.push(TaskQueueType::Nexus);
        }
        let heartbeat = self
            .client_worker_registrator
            .heartbeat_manager
            .as_ref()
            .and_then(|hm| (hm.heartbeat_callback)());
        let handle = tokio::spawn(async move {
            match client
                .shutdown_worker(sticky_name, task_queue, task_queue_types, heartbeat)
                .await
            {
                Err(err)
                    if !matches!(
                        err.code(),
                        tonic::Code::Unimplemented | tonic::Code::Unavailable
                    ) =>
                {
                    warn!(
                        "shutdown_worker rpc errored during worker shutdown: {:?}",
                        err
                    );
                }
                _ => {}
            }
        });
        *guard = Some(handle);
    }

    /// Unique identifier for this worker instance.
    /// This must be stable across the worker's lifetime and unique per instance.
    pub fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    /// Sets a function to be called at the end of each activation completion
    pub(crate) fn set_post_activate_hook(
        &mut self,
        callback: impl Fn(&Self, PostActivateHookData<'_>) + Send + Sync + 'static,
    ) {
        self.post_activate_hook = Some(Box::new(callback))
    }

    fn complete_local_act(&self, task_token: TaskToken, la_res: LocalActivityExecutionResult) {
        if let Some(la_mgr) = &self.local_act_mgr
            && self
                .handle_la_complete_action(la_mgr.complete(&task_token, la_res))
                .is_some()
        {
            dbg_panic!("Should never be a task from direct completion");
        }
    }

    fn handle_la_complete_action(&self, action: LACompleteAction) -> Option<ActivityTask> {
        match action {
            LACompleteAction::Report {
                run_id,
                resolution,
                task,
            } => {
                self.notify_local_result(&run_id, LocalResolution::LocalActivity(resolution));
                task
            }
            LACompleteAction::WillBeRetried(task) => task,
            LACompleteAction::Untracked => None,
        }
    }

    fn notify_local_result(&self, run_id: &str, res: LocalResolution) {
        if let Some(workflows) = self.task_subsystems.workflows.as_ref() {
            workflows.notify_of_local_result(run_id, res);
        } else {
            dbg_panic!("trying to notify local result when workflows not enabled for this worker");
        }
    }
}

/// Errors thrown by [crate::Worker::validate]
#[derive(thiserror::Error, Debug)]
pub enum WorkerValidationError {
    /// The namespace provided to the worker does not exist on the server.
    #[error("Namespace {namespace} was not found or otherwise could not be described: {source:?}")]
    NamespaceDescribeError {
        /// The underlying server error.
        source: tonic::Status,
        /// The associated namespace.
        namespace: String,
    },
}

/// Errors thrown by [crate::Worker] polling methods
#[derive(thiserror::Error, Debug)]
pub enum PollError {
    /// [crate::Worker::shutdown] was called, and there are no more tasks to be handled from this
    /// poll function. Lang must call [crate::Worker::complete_workflow_activation],
    /// [crate::Worker::complete_activity_task], or
    /// [crate::Worker::complete_nexus_task] for any remaining tasks, and then may exit.
    #[error("Core is shut down and there are no more tasks of this kind")]
    ShutDown,
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when polling: {0:?}")]
    TonicError(#[from] tonic::Status),
}

/// Errors thrown by [crate::Worker::complete_workflow_activation]
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteWfError {
    /// Lang SDK sent us a malformed workflow completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed workflow completion for run ({run_id}): {reason}")]
    MalformedWorkflowCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The run associated with the completion
        run_id: String,
    },
    /// Workflows have not been enabled on this worker.
    #[error("Workflows are not enabled on this worker")]
    WorkflowNotEnabled,
}

/// Errors thrown by [crate::Worker::complete_activity_task]
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteActivityError {
    /// Lang SDK sent us a malformed activity completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed activity completion ({reason}): {completion:?}")]
    MalformedActivityCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<ActivityExecutionResult>,
    },
    /// Activities have not been enabled on this worker.
    #[error("Activities are not enabled on this worker")]
    ActivityNotEnabled,
}

/// Errors thrown by [crate::Worker::complete_nexus_task]
#[derive(thiserror::Error, Debug)]
pub enum CompleteNexusError {
    /// Lang SDK sent us a malformed nexus completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed nexus completion: {reason}")]
    MalformedNexusCompletion {
        /// Reason the completion was malformed
        reason: String,
    },
    /// Nexus has not been enabled on this worker. If a user registers any Nexus handlers, the
    #[error("Nexus is not enabled on this worker")]
    NexusNotEnabled,
}

/// Errors we can encounter during workflow processing which we may treat as either WFT failures
/// or whole-workflow failures depending on user preference.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum WorkflowErrorType {
    /// A nondeterminism error
    Nondeterminism,
}

/// This trait allows users to customize the performance characteristics of workers dynamically.
/// For more, see the docstrings of the traits in the return types of its functions.
pub trait WorkerTuner {
    /// Return a [SlotSupplier] for workflow tasks. Note that workflow task slot suppliers must be
    /// willing to hand out a minimum of one non-sticky slot and one sticky slot if workflow caching
    /// is enabled, otherwise the worker may fail to process new tasks.
    fn workflow_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for activity tasks
    fn activity_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for local activities
    fn local_activity_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync>;

    /// Return a [SlotSupplier] for nexus tasks
    fn nexus_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync>;
}

/// Implementing this trait allows users to customize how many tasks of certain kinds the worker
/// will perform concurrently.
///
/// Note that, for implementations on workflow tasks ([WorkflowSlotKind]), workers that have the
/// workflow cache enabled should be willing to hand out _at least_ two slots, to avoid the worker
/// becoming stuck only polling on the worker's sticky queue.
#[async_trait::async_trait]
pub trait SlotSupplier {
    /// The kind of slot this supplier is supplying.
    type SlotKind: SlotKind;
    /// Block until a slot is available, then return a permit for the slot.
    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit;

    /// Try to immediately reserve a slot, returning None if one is not available. Implementations
    /// must not block, or risk blocking the async event loop.
    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit>;

    /// Marks a slot as actually now being used. This is separate from reserving one because the
    /// pollers need to reserve a slot before they have actually obtained work from server. Once
    /// that task is obtained (and validated) then the slot can actually be used to work on the
    /// task.
    ///
    /// Users' implementation of this can choose to emit metrics, or otherwise leverage the
    /// information provided by the `info` parameter to be better able to make future decisions
    /// about whether a slot should be handed out.
    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>);

    /// Frees a slot.
    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>);

    /// If this implementation knows how many slots are available at any moment, it should return
    /// that here.
    fn available_slots(&self) -> Option<usize> {
        None
    }

    /// Returns a human-friendly identifier describing this supplier implementation for
    /// diagnostics and telemetry.
    fn slot_supplier_kind(&self) -> String {
        "Custom".to_string()
    }
}

/// Context for slot reservation.
pub trait SlotReservationContext: Send + Sync {
    /// Returns the name of the task queue this worker is polling
    fn task_queue(&self) -> &str;

    /// Returns the identity of the worker
    fn worker_identity(&self) -> &str;

    /// Returns the deployment version of the worker, if one is set.
    fn worker_deployment_version(&self) -> &Option<WorkerDeploymentVersion>;

    /// Returns the number of currently outstanding slot permits, whether used or un-used.
    fn num_issued_slots(&self) -> usize;

    /// Returns true iff this is a sticky poll for a workflow task
    fn is_sticky(&self) -> bool;

    /// Returns the metrics meter if metrics are enabled
    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        None
    }
}

/// Context for slots being marked as used.
pub trait SlotMarkUsedContext: Send + Sync {
    /// The kind of slot being marked used.
    type SlotKind: SlotKind;
    /// The slot permit that is being used
    fn permit(&self) -> &SlotSupplierPermit;
    /// Returns the info of slot that was marked as used
    fn info(&self) -> &<Self::SlotKind as SlotKind>::Info;

    /// Returns the metrics meter if metrics are enabled
    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        None
    }
}

/// Context for slots being released.
pub trait SlotReleaseContext: Send + Sync {
    /// The kind of slot being marked released.
    type SlotKind: SlotKind;
    /// The slot permit that is being used
    fn permit(&self) -> &SlotSupplierPermit;
    /// Returns the info of slot that was released, if it was used
    fn info(&self) -> Option<&<Self::SlotKind as SlotKind>::Info>;

    /// Returns the metrics meter if metrics are enabled
    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        None
    }
}

/// A permit issued by a [SlotSupplier].
#[derive(Default, Debug)]
pub struct SlotSupplierPermit {
    user_data: Option<Box<dyn Any + Send + Sync>>,
}
impl SlotSupplierPermit {
    /// Attach some user data to the slot permit.
    pub fn with_user_data<T: Any + Send + Sync>(user_data: T) -> Self {
        Self {
            user_data: Some(Box::new(user_data)),
        }
    }
    /// Attempts to downcast the inner data, if any, into the provided type and returns it.
    /// Returns none if there is no data or the data is not of the appropriate type.
    pub fn user_data<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.user_data.as_ref().and_then(|b| b.downcast_ref())
    }
    /// Attempts to downcast the inner data, if any, into the provided type and returns it mutably.
    /// Returns none if there is no data or the data is not of the appropriate type.
    pub fn user_data_mut<T: Any + Send + Sync>(&mut self) -> Option<&mut T> {
        self.user_data.as_mut().and_then(|b| b.downcast_mut())
    }
}

/// What kind of task the slot is used for.
#[derive(Debug, Copy, Clone, derive_more::Display, Eq, PartialEq)]
pub enum SlotKindType {
    /// Workflow tasks.
    Workflow,
    /// Activity tasks.
    Activity,
    /// Local activity tasks.
    LocalActivity,
    /// Nexus tasks.
    Nexus,
}

/// Marker struct for workflow slots.
#[derive(Debug, Copy, Clone)]
pub struct WorkflowSlotKind {}
/// Marker struct for activity slots.
#[derive(Debug, Copy, Clone)]
pub struct ActivitySlotKind {}
/// Marker struct for local activity slots.
#[derive(Debug, Copy, Clone)]
pub struct LocalActivitySlotKind {}
/// Marker struct for nexus slots.
#[derive(Debug, Copy, Clone)]
pub struct NexusSlotKind {}

/// Contextual information about in-use slots.
pub enum SlotInfo<'a> {
    /// For workflow slots.
    Workflow(&'a WorkflowSlotInfo),
    /// For activity slots.
    Activity(&'a ActivitySlotInfo),
    /// For local activity slots.
    LocalActivity(&'a LocalActivitySlotInfo),
    /// For nexus slots.
    Nexus(&'a NexusSlotInfo),
}

/// Allows reifying slot info into the appropriate type.
pub trait SlotInfoTrait: prost::Message {
    /// Downcast a protobuf message into the enum.
    fn downcast(&self) -> SlotInfo<'_>;
}
impl SlotInfoTrait for WorkflowSlotInfo {
    fn downcast(&self) -> SlotInfo<'_> {
        SlotInfo::Workflow(self)
    }
}
impl SlotInfoTrait for ActivitySlotInfo {
    fn downcast(&self) -> SlotInfo<'_> {
        SlotInfo::Activity(self)
    }
}
impl SlotInfoTrait for LocalActivitySlotInfo {
    fn downcast(&self) -> SlotInfo<'_> {
        SlotInfo::LocalActivity(self)
    }
}
impl SlotInfoTrait for NexusSlotInfo {
    fn downcast(&self) -> SlotInfo<'_> {
        SlotInfo::Nexus(self)
    }
}

/// Associates slot info/kinds together.
pub trait SlotKind {
    /// The associated info for this kind.
    type Info: SlotInfoTrait;

    /// Return this kind.
    fn kind() -> SlotKindType;
}
impl SlotKind for WorkflowSlotKind {
    type Info = WorkflowSlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Workflow
    }
}
impl SlotKind for ActivitySlotKind {
    type Info = ActivitySlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Activity
    }
}
impl SlotKind for LocalActivitySlotKind {
    type Info = LocalActivitySlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::LocalActivity
    }
}
impl SlotKind for NexusSlotKind {
    type Info = NexusSlotInfo;

    fn kind() -> SlotKindType {
        SlotKindType::Nexus
    }
}

/// Different strategies for task polling
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PollerBehavior {
    /// Will attempt to poll as long as a slot is available, up to the provided maximum. Cannot
    /// be less than two for workflow tasks, or one for other tasks.
    SimpleMaximum(usize),
    /// Will automatically scale the number of pollers based on feedback from the server. Still
    /// requires a slot to be available before beginning polling.
    Autoscaling {
        /// At least this many poll calls will always be attempted (assuming slots are available).
        /// Cannot be zero.
        minimum: usize,
        /// At most this many poll calls will ever be open at once. Must be >= `minimum`.
        maximum: usize,
        /// This many polls will be attempted initially before scaling kicks in. Must be between
        /// `minimum` and `maximum`.
        initial: usize,
    },
}

impl PollerBehavior {
    /// Returns true if the behavior is using autoscaling.
    pub fn is_autoscaling(&self) -> bool {
        matches!(self, PollerBehavior::Autoscaling { .. })
    }

    /// Validates the behavior.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            PollerBehavior::SimpleMaximum(x) => {
                if *x < 1 {
                    return Err("SimpleMaximum poller behavior must be at least 1".to_owned());
                }
            }
            PollerBehavior::Autoscaling {
                minimum,
                maximum,
                initial,
            } => {
                if *minimum < 1 {
                    return Err("Autoscaling minimum poller behavior must be at least 1".to_owned());
                }
                if *maximum < *minimum {
                    return Err(
                        "Autoscaling maximum must be greater than or equal to minimum".to_owned(),
                    );
                }
                if *initial < *minimum || *initial > *maximum {
                    return Err(
                        "Autoscaling initial must be between minimum and maximum".to_owned()
                    );
                }
            }
        }
        Ok(())
    }
}

/// Strategy a core worker uses for versioning.
#[derive(Clone, Debug)]
pub enum WorkerVersioningStrategy {
    /// Don't enable any versioning
    None {
        /// Build ID may still be passed as a way to identify the worker, or may be left empty.
        build_id: String,
    },
    /// Maybe use the modern deployment-based versioning, or just pass a deployment version.
    WorkerDeploymentBased(WorkerDeploymentOptions),
    /// Use the legacy build-id-based whole worker versioning.
    LegacyBuildIdBased {
        /// A Build ID to use, must be non-empty.
        build_id: String,
    },
}

impl Default for WorkerVersioningStrategy {
    fn default() -> Self {
        WorkerVersioningStrategy::None {
            build_id: String::new(),
        }
    }
}

impl WorkerVersioningStrategy {
    /// Return the build ID associated with this strategy.
    pub fn build_id(&self) -> &str {
        match self {
            WorkerVersioningStrategy::None { build_id } => build_id,
            WorkerVersioningStrategy::WorkerDeploymentBased(opts) => &opts.version.build_id,
            WorkerVersioningStrategy::LegacyBuildIdBased { build_id } => build_id,
        }
    }

    /// Returns true if this uses "build id based" legacy versioning.
    pub fn uses_build_id_based(&self) -> bool {
        matches!(self, WorkerVersioningStrategy::LegacyBuildIdBased { .. })
    }

    /// Returns the default versioning behavior associated with this strategy, if any.
    pub fn default_versioning_behavior(&self) -> Option<VersioningBehavior> {
        match self {
            WorkerVersioningStrategy::WorkerDeploymentBased(opts) => {
                opts.default_versioning_behavior.map(Into::into)
            }
            _ => None,
        }
    }
}

struct ClientWorkerRegistrator {
    worker_instance_key: Uuid,
    slot_provider: SlotProvider,
    heartbeat_manager: Option<WorkerHeartbeatManager>,
    /// Slot filled by the deferred poller build with the activity cancellation callback. Shared
    /// with the worker so it can be resolved lazily once the activity task manager is constructed.
    cancel_activity_slot: Arc<OnceLock<CancelActivityCallback>>,
    client: RwLock<Arc<dyn WorkerClient>>,
    namespace_description: Arc<NamespaceDescriptionSource>,
    shared_namespace_worker: bool,
    task_types: WorkerTaskTypes,
}

impl ClientWorker for ClientWorkerRegistrator {
    fn namespace(&self) -> &str {
        self.slot_provider.namespace()
    }
    fn task_queue(&self) -> &str {
        self.slot_provider.task_queue()
    }

    fn try_reserve_wft_slot(&self) -> Option<Box<dyn SlotTrait + Send>> {
        self.slot_provider.try_reserve_wft_slot()
    }

    fn deployment_options(&self) -> Option<temporalio_common::worker::WorkerDeploymentOptions> {
        self.slot_provider.deployment_options()
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn heartbeat_enabled(&self) -> bool {
        self.heartbeat_manager.is_some()
    }

    fn heartbeat_callback(&self) -> Option<HeartbeatCallback> {
        if let Some(hb_mgr) = self.heartbeat_manager.as_ref() {
            Some(hb_mgr.heartbeat_callback.clone())
        } else {
            None
        }
    }

    fn heartbeat_success_callback(&self) -> Option<HeartbeatSuccessCallback> {
        self.heartbeat_manager
            .as_ref()
            .map(|hb_mgr| hb_mgr.heartbeat_success_callback.clone())
    }

    fn cancel_activity_callback(&self) -> Option<CancelActivityCallback> {
        // When activities are disabled the slot is never filled, so the wrapper returns false (a
        // harmless no-op); no need to gate on whether activities are enabled.
        let slot = self.cancel_activity_slot.clone();
        Some(Arc::new(move |tt| {
            slot.get().map(|cb| cb(tt)).unwrap_or(false)
        }))
    }

    fn new_shared_namespace_worker(
        &self,
    ) -> Result<Box<dyn SharedNamespaceWorkerTrait + Send + Sync>, anyhow::Error> {
        if let Some(ref hb_mgr) = self.heartbeat_manager {
            Ok(Box::new(SharedNamespaceWorker::new(
                self.client.read().clone(),
                self.namespace().to_string(),
                hb_mgr.heartbeat_interval,
                hb_mgr.telemetry.clone(),
                self.namespace_description.clone(),
            )?))
        } else {
            bail!("Shared namespace worker creation never be called without a heartbeat manager");
        }
    }

    fn worker_task_types(&self) -> WorkerTaskTypes {
        self.task_types
    }
}

struct HeartbeatMetrics {
    in_mem_metrics: Option<Arc<WorkerHeartbeatMetrics>>,
    wft_slots: MeteredPermitDealer<WorkflowSlotKind>,
    act_slots: MeteredPermitDealer<ActivitySlotKind>,
    nexus_slots: MeteredPermitDealer<NexusSlotKind>,
    la_slots: MeteredPermitDealer<LocalActivitySlotKind>,
    wf_last_suc_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    wf_sticky_last_suc_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    act_last_suc_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    nexus_last_suc_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
    status: Arc<RwLock<WorkerStatus>>,
    sys_info: Arc<dyn SystemResourceInfo + Send + Sync>,
}

struct WorkerHeartbeatManager {
    /// Heartbeat interval, defaults to 60s
    heartbeat_interval: Duration,
    /// Telemetry instance, needed to initialize [SharedNamespaceWorker] when replacing client
    telemetry: Option<WorkerTelemetry>,
    /// Heartbeat callback
    heartbeat_callback: HeartbeatCallback,
    heartbeat_success_callback: HeartbeatSuccessCallback,
    start_time: Arc<OnceLock<prost_types::Timestamp>>,
}

impl WorkerHeartbeatManager {
    fn new(
        config: WorkerConfig,
        worker_instance_key: Uuid,
        heartbeat_interval: Duration,
        telemetry_instance: Option<WorkerTelemetry>,
        heartbeat_manager_metrics: HeartbeatMetrics,
        capabilities: Arc<NamespaceCapabilities>,
        environment_info: Option<Arc<EnvironmentInfo>>,
    ) -> Self {
        let start_time = Arc::new(OnceLock::new());
        let heartbeat_start_time = start_time.clone();
        let environment_info = Arc::new(Mutex::new(environment_info));
        let heartbeat_environment_info = environment_info.clone();
        let worker_heartbeat_callback: HeartbeatCallback = Arc::new(move || {
            let start_time = heartbeat_start_time.get().copied()?;
            let deployment_version = config.computed_deployment_version().map(|dv| {
                deployment::v1::WorkerDeploymentVersion {
                    deployment_name: dv.deployment_name,
                    build_id: dv.build_id,
                }
            });

            let mut plugins: Vec<_> = config.plugins.clone().into_iter().collect();
            plugins.sort_by(|a, b| a.name.cmp(&b.name));

            let mut drivers: Vec<_> = config.storage_drivers.clone().into_iter().collect();
            drivers.sort_by(|a, b| a.r#type.cmp(&b.r#type));

            let mut worker_heartbeat = WorkerHeartbeat {
                worker_instance_key: worker_instance_key.to_string(),
                host_info: Some(WorkerHostInfo {
                    host_name: gethostname().to_string_lossy().to_string(),
                    process_id: std::process::id().to_string(),
                    current_host_cpu_usage: heartbeat_manager_metrics.sys_info.used_cpu_percent()
                        as f32,
                    current_host_mem_usage: heartbeat_manager_metrics.sys_info.used_mem_percent()
                        as f32,

                    // Set by SharedNamespaceWorker because it relies on the client
                    worker_grouping_key: String::new(),
                }),
                task_queue: config.task_queue.clone(),
                deployment_version,

                status: (*heartbeat_manager_metrics.status.read()) as i32,
                start_time: Some(start_time),
                plugins,
                drivers,
                environment: heartbeat_environment_info.lock().as_deref().cloned(),

                // Some Metrics dependent fields are set below, and
                // some fields like sdk_name, sdk_version, and worker_identity, must be set by
                // SharedNamespaceWorker because they rely on the client, and
                // need to be pulled from the current client used by SharedNamespaceWorker
                ..Default::default()
            };

            if let Some(in_mem) = heartbeat_manager_metrics.in_mem_metrics.as_ref() {
                worker_heartbeat.total_sticky_cache_hit =
                    in_mem.total_sticky_cache_hit.load(Ordering::Relaxed) as i32;
                worker_heartbeat.total_sticky_cache_miss =
                    in_mem.total_sticky_cache_miss.load(Ordering::Relaxed) as i32;
                worker_heartbeat.current_sticky_cache_size =
                    in_mem.sticky_cache_size.load(Ordering::Relaxed) as i32;

                worker_heartbeat.workflow_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .wft_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: heartbeat_manager_metrics
                        .wf_last_suc_poll_time
                        .load()
                        .map(|time| time.into()),
                    is_autoscaling: resolve_effective_behavior(
                        config.workflow_task_poller_behavior,
                        &capabilities,
                    )
                    .is_autoscaling(),
                });
                worker_heartbeat.workflow_sticky_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .sticky_wft_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: heartbeat_manager_metrics
                        .wf_sticky_last_suc_poll_time
                        .load()
                        .map(|time| time.into()),
                    is_autoscaling: resolve_effective_behavior(
                        config.workflow_task_poller_behavior,
                        &capabilities,
                    )
                    .is_autoscaling(),
                });
                worker_heartbeat.activity_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .activity_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: heartbeat_manager_metrics
                        .act_last_suc_poll_time
                        .load()
                        .map(|time| time.into()),
                    is_autoscaling: resolve_effective_behavior(
                        config.activity_task_poller_behavior,
                        &capabilities,
                    )
                    .is_autoscaling(),
                });
                worker_heartbeat.nexus_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .nexus_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: heartbeat_manager_metrics
                        .nexus_last_suc_poll_time
                        .load()
                        .map(|time| time.into()),
                    is_autoscaling: resolve_effective_behavior(
                        config.nexus_task_poller_behavior,
                        &capabilities,
                    )
                    .is_autoscaling(),
                });

                worker_heartbeat.workflow_task_slots_info = make_slots_info(
                    &heartbeat_manager_metrics.wft_slots,
                    in_mem.worker_task_slots_available.workflow_worker.clone(),
                    in_mem.worker_task_slots_used.workflow_worker.clone(),
                    in_mem.workflow_task_execution_latency.clone(),
                    in_mem.workflow_task_execution_failed.clone(),
                );
                worker_heartbeat.activity_task_slots_info = make_slots_info(
                    &heartbeat_manager_metrics.act_slots,
                    in_mem.worker_task_slots_available.activity_worker.clone(),
                    in_mem.worker_task_slots_used.activity_worker.clone(),
                    in_mem.activity_execution_latency.clone(),
                    in_mem.activity_execution_failed.clone(),
                );
                worker_heartbeat.nexus_task_slots_info = make_slots_info(
                    &heartbeat_manager_metrics.nexus_slots,
                    in_mem.worker_task_slots_available.nexus_worker.clone(),
                    in_mem.worker_task_slots_used.nexus_worker.clone(),
                    in_mem.nexus_task_execution_latency.clone(),
                    in_mem.nexus_task_execution_failed.clone(),
                );
                worker_heartbeat.local_activity_slots_info = make_slots_info(
                    &heartbeat_manager_metrics.la_slots,
                    in_mem
                        .worker_task_slots_available
                        .local_activity_worker
                        .clone(),
                    in_mem.worker_task_slots_used.local_activity_worker.clone(),
                    in_mem.local_activity_execution_latency.clone(),
                    in_mem.local_activity_execution_failed.clone(),
                );
            }
            Some(worker_heartbeat)
        });
        let heartbeat_success_callback: HeartbeatSuccessCallback = Arc::new(move || {
            // Take the env info because we only care about sending it one time
            environment_info.lock().take();
        });

        WorkerHeartbeatManager {
            heartbeat_interval,
            telemetry: telemetry_instance,
            heartbeat_callback: worker_heartbeat_callback,
            heartbeat_success_callback,
            start_time,
        }
    }

    fn mark_started(&self) {
        let _ = self.start_time.set(SystemTime::now().into());
    }
}

pub(crate) struct PostActivateHookData<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) replaying: bool,
}

pub(crate) enum TaskPollers {
    Real,
    #[cfg(any(feature = "test-utilities", test))]
    Mocked {
        wft_stream: Option<BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>>,
        act_poller: Option<BoxedPoller<PollActivityTaskQueueResponse>>,
        nexus_poller: Option<BoxedPoller<PollNexusTaskQueueResponse>>,
    },
}

/// Given an already-resolved workflow task poller `behavior` (see [resolve_effective_behavior]),
/// applies the sticky/non-sticky split when it is a [PollerBehavior::SimpleMaximum]; other
/// behaviors are returned unchanged.
pub(crate) fn wft_poller_behavior(
    behavior: PollerBehavior,
    config: &WorkerConfig,
    is_sticky: bool,
) -> PollerBehavior {
    fn calc_max_nonsticky(max_polls: usize, ratio: f32) -> usize {
        ((max_polls as f32 * ratio) as usize).max(1)
    }

    if let PollerBehavior::SimpleMaximum(m) = behavior {
        if !is_sticky {
            PollerBehavior::SimpleMaximum(calc_max_nonsticky(
                m,
                config.nonsticky_to_sticky_poll_ratio,
            ))
        } else {
            PollerBehavior::SimpleMaximum(
                m.saturating_sub(calc_max_nonsticky(m, config.nonsticky_to_sticky_poll_ratio))
                    .max(1),
            )
        }
    } else {
        behavior
    }
}

fn make_slots_info<SK>(
    dealer: &MeteredPermitDealer<SK>,
    slots_available: Arc<AtomicU64>,
    slots_used: Arc<AtomicU64>,
    total_processed: Arc<AtomicU64>,
    total_failed: Arc<AtomicU64>,
) -> Option<WorkerSlotsInfo>
where
    SK: SlotKind + 'static,
{
    Some(WorkerSlotsInfo {
        current_available_slots: i32::try_from(slots_available.load(Ordering::Relaxed))
            .unwrap_or(-1),
        current_used_slots: i32::try_from(slots_used.load(Ordering::Relaxed)).unwrap_or(-1),
        slot_supplier_kind: dealer.slot_supplier_kind().to_string(),
        total_processed_tasks: i32::try_from(total_processed.load(Ordering::Relaxed))
            .unwrap_or(i32::MIN),
        total_failed_tasks: i32::try_from(total_failed.load(Ordering::Relaxed)).unwrap_or(i32::MIN),

        // Filled in by heartbeat later
        last_interval_processed_tasks: 0,
        last_interval_failure_tasks: 0,
    })
}

fn worker_control_task_queue(namespace: &str, grouping_key: &str) -> String {
    format!("temporal-sys/worker-commands/{namespace}/{grouping_key}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        advance_fut,
        test_help::test_worker_cfg,
        worker::{
            PollerBehavior,
            client::mocks::{mock_manual_worker_client, mock_worker_client},
        },
    };
    use futures_util::FutureExt;
    use temporalio_common::protos::temporal::api::{
        namespace::v1::namespace_info::Capabilities,
        workflowservice::v1::PollActivityTaskQueueResponse,
    };

    #[tokio::test]
    async fn activity_timeouts_maintain_permit() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(PollActivityTaskQueueResponse::default()));

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        let fut = worker.poll_activity_task();
        advance_fut!(fut);
        assert_eq!(
            worker
                .task_subsystems
                .at_task_mgr
                .as_ref()
                .unwrap()
                .unused_permits(),
            Some(5)
        );
    }

    #[tokio::test]
    async fn activity_errs_dont_eat_permits() {
        // Return one error followed by simulating waiting on the poll, otherwise the poller will
        // loop very fast and be in some indeterminate state.
        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| async { Err(tonic::Status::internal("ahhh")) }.boxed())
            .times(1);
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| future::pending().boxed());

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert!(worker.activity_poll().await.is_err());
        assert_eq!(
            worker
                .task_subsystems
                .at_task_mgr
                .as_ref()
                .unwrap()
                .unused_permits(),
            Some(5)
        );
    }

    #[test]
    fn max_polls_calculated_properly() {
        let cfg = {
            let mut cfg = test_worker_cfg().build().unwrap();
            cfg.workflow_task_poller_behavior = Some(PollerBehavior::SimpleMaximum(5_usize));
            cfg
        };
        assert_eq!(
            wft_poller_behavior(PollerBehavior::SimpleMaximum(5), &cfg, false),
            PollerBehavior::SimpleMaximum(1)
        );
        assert_eq!(
            wft_poller_behavior(PollerBehavior::SimpleMaximum(5), &cfg, true),
            PollerBehavior::SimpleMaximum(4)
        );
    }

    #[test]
    fn max_polls_zero_is_err() {
        assert!(
            WorkerConfig::builder()
                .namespace("test")
                .task_queue("test")
                .versioning_strategy(WorkerVersioningStrategy::None {
                    build_id: "test".to_string(),
                })
                .task_types(WorkerTaskTypes::all())
                .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(0_usize))
                .build()
                .is_err()
        );
    }

    fn default_versioning_strategy() -> WorkerVersioningStrategy {
        WorkerVersioningStrategy::None {
            build_id: String::new(),
        }
    }

    #[test]
    fn test_default_configuration_polls_all_types() {
        let config = WorkerConfig::builder()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(default_versioning_strategy())
            .task_types(WorkerTaskTypes::all())
            .build()
            .unwrap();

        let effective = &config.task_types;
        assert!(
            effective.enable_workflows,
            "Should poll workflows by default"
        );
        assert!(
            effective.enable_local_activities,
            "should poll local activities by default"
        );
        assert!(
            effective.enable_remote_activities,
            "Should poll remote activities by default"
        );
        assert!(effective.enable_nexus, "Should poll nexus by default");
    }

    #[test]
    fn test_invalid_task_types_fails_validation() {
        // empty task types
        let result = WorkerConfig::builder()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(default_versioning_strategy())
            .task_types(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: false,
                enable_remote_activities: false,
                enable_nexus: false,
            })
            .build();

        assert!(result.is_err(), "Empty task_types should fail validation");
        let err = result.err().unwrap();
        assert!(
            err.contains("At least one task type"),
            "Error should mention task types: {err}",
        );

        // local activities with no workflows
        let result = WorkerConfig::builder()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(default_versioning_strategy())
            .task_types(WorkerTaskTypes {
                enable_workflows: false,
                enable_local_activities: true,
                enable_remote_activities: false,
                enable_nexus: false,
            })
            .build();

        assert!(result.is_err(), "Empty task_types should fail validation");
        let err = result.err().unwrap();
        assert!(
            err.contains("cannot enable local activities without workflows"),
            "Error should mention task types: {err}",
        );
    }

    #[test]
    fn test_all_combinations() {
        let combinations = [
            (WorkerTaskTypes::workflow_only(), "workflows only"),
            (WorkerTaskTypes::activity_only(), "activities only"),
            (WorkerTaskTypes::nexus_only(), "nexus only"),
            (
                WorkerTaskTypes {
                    enable_workflows: true,
                    enable_local_activities: true,
                    enable_remote_activities: true,
                    enable_nexus: false,
                },
                "workflows + activities",
            ),
            (
                WorkerTaskTypes {
                    enable_workflows: true,
                    enable_local_activities: true,
                    enable_remote_activities: false,
                    enable_nexus: true,
                },
                "workflows + nexus",
            ),
            (
                WorkerTaskTypes {
                    enable_workflows: false,
                    enable_local_activities: false,
                    enable_remote_activities: true,
                    enable_nexus: true,
                },
                "activities + nexus",
            ),
            (WorkerTaskTypes::all(), "all types"),
        ];

        for (task_types, description) in combinations {
            let config = WorkerConfig::builder()
                .namespace("default")
                .task_queue("test-queue")
                .versioning_strategy(default_versioning_strategy())
                .task_types(task_types)
                .build()
                .unwrap();

            let effective = config.task_types;
            assert_eq!(
                effective, task_types,
                "Effective types should match for {description}",
            );
        }
    }

    #[test]
    fn rejects_versioning_behavior_when_versioning_off() {
        let result = WorkerConfig::builder()
            .namespace("default")
            .task_queue("test-queue")
            .versioning_strategy(WorkerVersioningStrategy::WorkerDeploymentBased(
                WorkerDeploymentOptions::new(WorkerDeploymentVersion {
                    deployment_name: "deployment".to_string(),
                    build_id: "1.0".to_string(),
                })
                .default_versioning_behavior(VersioningBehavior::AutoUpgrade.into())
                .build(),
            ))
            .task_types(WorkerTaskTypes::all())
            .build();
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            err.contains("default_versioning_behavior must be None"),
            "Error should mention default_versioning_behavior: {err}",
        );
    }

    fn auto_enroll_caps() -> NamespaceCapabilities {
        NamespaceCapabilities::resolved(Capabilities {
            poller_autoscaling_auto_enroll: true,
            ..Default::default()
        })
    }

    #[test]
    fn resolve_effective_behavior_enrolls_unset_with_capability() {
        let caps = auto_enroll_caps();
        assert_eq!(
            resolve_effective_behavior(None, &caps),
            PollerBehavior::Autoscaling {
                minimum: 1,
                maximum: 100,
                initial: 5,
            },
        );
    }

    #[test]
    fn resolve_effective_behavior_unset_without_capability_defaults() {
        let caps = NamespaceCapabilities::default();
        assert_eq!(
            resolve_effective_behavior(None, &caps),
            PollerBehavior::SimpleMaximum(5),
        );
    }

    #[test]
    fn resolve_effective_behavior_configured_unchanged() {
        // A configured behavior is always used as-is, even when the capability is present.
        let caps = auto_enroll_caps();
        assert_eq!(
            resolve_effective_behavior(Some(PollerBehavior::SimpleMaximum(5)), &caps),
            PollerBehavior::SimpleMaximum(5),
        );
        let autoscaling = PollerBehavior::Autoscaling {
            minimum: 2,
            maximum: 20,
            initial: 4,
        };
        assert_eq!(
            resolve_effective_behavior(Some(autoscaling), &caps),
            autoscaling,
        );
    }
}
