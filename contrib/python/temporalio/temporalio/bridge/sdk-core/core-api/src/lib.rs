#[cfg(feature = "envconfig")]
pub mod envconfig;
pub mod errors;
pub mod telemetry;
pub mod worker;

use crate::{
    errors::{
        CompleteActivityError, CompleteNexusError, CompleteWfError, PollError,
        WorkerValidationError,
    },
    worker::WorkerConfig,
};
use temporal_sdk_core_protos::coresdk::{
    ActivityHeartbeat, ActivityTaskCompletion,
    activity_task::ActivityTask,
    nexus::{NexusTask, NexusTaskCompletion},
    workflow_activation::WorkflowActivation,
    workflow_completion::WorkflowActivationCompletion,
};

/// This trait is the primary way by which language specific SDKs interact with the core SDK.
/// It represents one worker, which has a (potentially shared) client for connecting to the service
/// and is bound to a specific task queue.
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    /// Validate that the worker can properly connect to server, plus any other validation that
    /// needs to be done asynchronously. Lang SDKs should call this function once before calling
    /// any others.
    async fn validate(&self) -> Result<(), WorkerValidationError>;

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
    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollError>;

    /// Ask the worker for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// Do not call poll concurrently. It handles polling the server concurrently internally.
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollError>;

    /// Ask the worker for some nexus related work. It is then the language SDK's
    /// responsibility to call the appropriate nexus operation handler code with the provided
    /// inputs. Blocks indefinitely until such work is available or [Worker::shutdown] is called.
    ///
    /// All tasks must be responded to for shutdown to complete.
    ///
    /// Do not call poll concurrently. It handles polling the server concurrently internally.
    async fn poll_nexus_task(&self) -> Result<NexusTask, PollError>;

    /// Tell the worker that a workflow activation has completed. May (and should) be freely called
    /// concurrently. The future may take some time to resolve, as fetching more events might be
    /// necessary for completion to... complete - thus SDK implementers should make sure they do
    /// not serialize completions.
    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError>;

    /// Tell the worker that an activity has finished executing. May (and should) be freely called
    /// concurrently.
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError>;

    /// Tell the worker that a nexus task has completed. May (and should) be freely called
    /// concurrently.
    async fn complete_nexus_task(
        &self,
        completion: NexusTaskCompletion,
    ) -> Result<(), CompleteNexusError>;

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
    /// For now activity still need to send heartbeats if they want to receive cancellation
    /// requests. In the future we will change this and will dispatch cancellations more
    /// proactively. Note that this function does not block on the server call and returns
    /// immediately. Underlying validation errors are swallowed and logged, this has been agreed to
    /// be optimal behavior for the user as we don't want to break activity execution due to badly
    /// configured heartbeat options.
    fn record_activity_heartbeat(&self, details: ActivityHeartbeat);

    /// Request that a workflow be evicted by its run id. This will generate a workflow activation
    /// with the eviction job inside it to be eventually returned by
    /// [Worker::poll_workflow_activation]. If the workflow had any existing outstanding activations,
    /// such activations are invalidated and subsequent completions of them will do nothing and log
    /// a warning.
    fn request_workflow_eviction(&self, run_id: &str);

    /// Return this worker's config
    fn get_config(&self) -> &WorkerConfig;

    /// Initiate shutdown. See [Worker::shutdown], this is just a sync version that starts the
    /// process. You can then wait on `shutdown` or [Worker::finalize_shutdown].
    fn initiate_shutdown(&self);

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down this worker. [Worker::poll_workflow_activation] and [Worker::poll_activity_task] should
    /// be called until both return a `ShutDown` error to ensure that all outstanding work is
    /// complete. This means that the lang sdk will need to call
    /// [Worker::complete_workflow_activation] and [Worker::complete_activity_task] for those
    /// workflows & activities until they are done. At that point, the lang SDK can end the process,
    /// or drop the [Worker] instance via [Worker::finalize_shutdown], which will close the
    /// connection and free resources. If you have set [WorkerConfig::no_remote_activities], you may
    /// skip calling [Worker::poll_activity_task].
    ///
    /// Lang implementations should use [Worker::initiate_shutdown] followed by
    /// [Worker::finalize_shutdown].
    async fn shutdown(&self);

    /// Completes shutdown and frees all resources. You should avoid simply dropping workers, as
    /// this does not allow async tasks to report any panics that may have occurred cleanly.
    ///
    /// This should be called only after [Worker::shutdown] has resolved and/or both polling
    /// functions have returned `ShutDown` errors.
    async fn finalize_shutdown(self);
}

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;
