use std::{collections::HashMap, time::Duration};

use crate::{MemoValues, WorkflowCancellationToken, runtime::types::ContinueAsNewRequest};
use temporalio_common_wasm::{
    ActivityCloseTimeouts, Priority, RetryPolicy,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::{
        coresdk::{
            child_workflow::{
                ChildWorkflowCancellationType as ProtoChildWorkflowCancellationType,
                ParentClosePolicy as ProtoParentClosePolicy,
            },
            common::VersioningIntent as ProtoVersioningIntent,
            nexus::NexusOperationCancellationType as ProtoNexusOperationCancellationType,
            workflow_commands::{
                ActivityCancellationType as ProtoActivityCancellationType,
                ContinueAsNewWorkflowExecution, ScheduleActivity, ScheduleLocalActivity,
                ScheduleNexusOperation, SignalExternalWorkflowExecution,
                StartChildWorkflowExecution, StartTimer, WorkflowCommand,
                signal_external_workflow_execution, workflow_command,
            },
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::{
                ContinueAsNewVersioningBehavior as ProtoContinueAsNewVersioningBehavior,
                WorkflowIdReusePolicy as ProtoWorkflowIdReusePolicy,
            },
            sdk::v1::{EventGroupMarker, UserMetadata},
        },
    },
    search_attributes::SearchAttributes,
};

/// Controls when activity cancellation is reported back to a workflow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum ActivityCancellationType {
    /// Request cancellation and report it immediately.
    #[default]
    TryCancel,
    /// Wait until cancellation has completed.
    WaitCancellationCompleted,
    /// Do not request cancellation.
    Abandon,
}

impl From<ActivityCancellationType> for ProtoActivityCancellationType {
    fn from(value: ActivityCancellationType) -> Self {
        match value {
            ActivityCancellationType::TryCancel => Self::TryCancel,
            ActivityCancellationType::WaitCancellationCompleted => Self::WaitCancellationCompleted,
            ActivityCancellationType::Abandon => Self::Abandon,
        }
    }
}

impl From<ProtoActivityCancellationType> for ActivityCancellationType {
    fn from(value: ProtoActivityCancellationType) -> Self {
        match value {
            ProtoActivityCancellationType::TryCancel => Self::TryCancel,
            ProtoActivityCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            ProtoActivityCancellationType::Abandon => Self::Abandon,
        }
    }
}

/// Controls when child-workflow cancellation is reported to its parent.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum ChildWorkflowCancellationType {
    /// Do not request cancellation.
    Abandon,
    /// Request cancellation and report it immediately.
    TryCancel,
    /// Wait until cancellation has completed.
    #[default]
    WaitCancellationCompleted,
    /// Wait until the cancellation request is acknowledged.
    WaitCancellationRequested,
}

impl From<ChildWorkflowCancellationType> for ProtoChildWorkflowCancellationType {
    fn from(value: ChildWorkflowCancellationType) -> Self {
        match value {
            ChildWorkflowCancellationType::Abandon => Self::Abandon,
            ChildWorkflowCancellationType::TryCancel => Self::TryCancel,
            ChildWorkflowCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            ChildWorkflowCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}

impl From<ProtoChildWorkflowCancellationType> for ChildWorkflowCancellationType {
    fn from(value: ProtoChildWorkflowCancellationType) -> Self {
        match value {
            ProtoChildWorkflowCancellationType::Abandon => Self::Abandon,
            ProtoChildWorkflowCancellationType::TryCancel => Self::TryCancel,
            ProtoChildWorkflowCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            ProtoChildWorkflowCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}

/// Controls what happens to a child workflow when its parent closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ParentClosePolicy {
    /// Let the server choose its default.
    #[default]
    Unspecified,
    /// Terminate the child workflow.
    Terminate,
    /// Leave the child workflow running.
    Abandon,
    /// Request cancellation of the child workflow.
    RequestCancel,
}

impl From<ParentClosePolicy> for ProtoParentClosePolicy {
    fn from(value: ParentClosePolicy) -> Self {
        match value {
            ParentClosePolicy::Unspecified => Self::Unspecified,
            ParentClosePolicy::Terminate => Self::Terminate,
            ParentClosePolicy::Abandon => Self::Abandon,
            ParentClosePolicy::RequestCancel => Self::RequestCancel,
        }
    }
}

impl From<ProtoParentClosePolicy> for ParentClosePolicy {
    fn from(value: ProtoParentClosePolicy) -> Self {
        match value {
            ProtoParentClosePolicy::Unspecified => Self::Unspecified,
            ProtoParentClosePolicy::Terminate => Self::Terminate,
            ProtoParentClosePolicy::Abandon => Self::Abandon,
            ProtoParentClosePolicy::RequestCancel => Self::RequestCancel,
        }
    }
}

/// Controls whether a closed workflow ID may be reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum WorkflowIdReusePolicy {
    /// Use the SDK default of allowing duplicate IDs.
    #[default]
    Unspecified,
    /// Allow the workflow ID to be reused.
    AllowDuplicate,
    /// Allow reuse only when the previous execution failed.
    AllowDuplicateFailedOnly,
    /// Reject reuse of the workflow ID.
    RejectDuplicate,
    /// Terminate a running execution before reusing the ID.
    TerminateIfRunning,
}

impl From<WorkflowIdReusePolicy> for ProtoWorkflowIdReusePolicy {
    #[allow(deprecated)]
    fn from(value: WorkflowIdReusePolicy) -> Self {
        match value {
            WorkflowIdReusePolicy::Unspecified => Self::Unspecified,
            WorkflowIdReusePolicy::AllowDuplicate => Self::AllowDuplicate,
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly => Self::AllowDuplicateFailedOnly,
            WorkflowIdReusePolicy::RejectDuplicate => Self::RejectDuplicate,
            WorkflowIdReusePolicy::TerminateIfRunning => Self::TerminateIfRunning,
        }
    }
}

impl From<ProtoWorkflowIdReusePolicy> for WorkflowIdReusePolicy {
    #[allow(deprecated)]
    fn from(value: ProtoWorkflowIdReusePolicy) -> Self {
        match value {
            ProtoWorkflowIdReusePolicy::Unspecified => Self::Unspecified,
            ProtoWorkflowIdReusePolicy::AllowDuplicate => Self::AllowDuplicate,
            ProtoWorkflowIdReusePolicy::AllowDuplicateFailedOnly => Self::AllowDuplicateFailedOnly,
            ProtoWorkflowIdReusePolicy::RejectDuplicate => Self::RejectDuplicate,
            ProtoWorkflowIdReusePolicy::TerminateIfRunning => Self::TerminateIfRunning,
        }
    }
}

/// Selects the worker versioning behavior intended for a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum VersioningIntent {
    /// Let Core choose the appropriate behavior.
    #[default]
    Unspecified,
    /// Prefer a worker compatible with the current worker.
    Compatible,
    /// Use the target task queue's default worker version.
    Default,
}

impl From<VersioningIntent> for ProtoVersioningIntent {
    fn from(value: VersioningIntent) -> Self {
        match value {
            VersioningIntent::Unspecified => Self::Unspecified,
            VersioningIntent::Compatible => Self::Compatible,
            VersioningIntent::Default => Self::Default,
        }
    }
}

impl From<ProtoVersioningIntent> for VersioningIntent {
    fn from(value: ProtoVersioningIntent) -> Self {
        match value {
            ProtoVersioningIntent::Unspecified => Self::Unspecified,
            ProtoVersioningIntent::Compatible => Self::Compatible,
            ProtoVersioningIntent::Default => Self::Default,
        }
    }
}

/// Controls when Nexus operation cancellation is reported to a workflow.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[non_exhaustive]
pub enum NexusOperationCancellationType {
    /// Wait until cancellation has completed.
    #[default]
    WaitCancellationCompleted,
    /// Do not request cancellation.
    Abandon,
    /// Request cancellation and report it immediately.
    TryCancel,
    /// Wait until the cancellation request is acknowledged.
    WaitCancellationRequested,
}

impl From<NexusOperationCancellationType> for ProtoNexusOperationCancellationType {
    fn from(value: NexusOperationCancellationType) -> Self {
        match value {
            NexusOperationCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            NexusOperationCancellationType::Abandon => Self::Abandon,
            NexusOperationCancellationType::TryCancel => Self::TryCancel,
            NexusOperationCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}

impl From<ProtoNexusOperationCancellationType> for NexusOperationCancellationType {
    fn from(value: ProtoNexusOperationCancellationType) -> Self {
        match value {
            ProtoNexusOperationCancellationType::WaitCancellationCompleted => {
                Self::WaitCancellationCompleted
            }
            ProtoNexusOperationCancellationType::Abandon => Self::Abandon,
            ProtoNexusOperationCancellationType::TryCancel => Self::TryCancel,
            ProtoNexusOperationCancellationType::WaitCancellationRequested => {
                Self::WaitCancellationRequested
            }
        }
    }
}
/// Options for scheduling an activity
#[derive(Debug, bon::Builder, Clone)]
#[non_exhaustive]
#[builder(start_fn = with_close_timeouts, on(String, into), state_mod(vis = "pub"))]
pub struct ActivityOptions {
    /// Timeouts for activity completion.
    ///
    /// See [`ActivityCloseTimeouts`] for the meaning of each timeout variant.
    #[builder(start_fn)]
    pub close_timeouts: ActivityCloseTimeouts,
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Task queue to schedule the activity in
    ///
    /// If `None`, use the same task queue as the parent workflow.
    pub task_queue: Option<String>,
    /// Time that the Activity Task can stay in the Task Queue before it is picked up by a Worker.
    /// Do not specify this timeout unless using host specific Task Queues for Activity Tasks are
    /// being used for routing.
    /// `schedule_to_start_timeout` is always non-retryable.
    /// Retrying after this timeout doesn't make sense as it would just put the Activity Task back
    /// into the same Task Queue.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Heartbeat interval. Activity must heartbeat before this interval passes after a last
    /// heartbeat or activity start.
    pub heartbeat_timeout: Option<Duration>,
    /// Determines what the SDK does when the Activity is cancelled.
    #[builder(default, into)]
    pub cancellation_type: ActivityCancellationType,
    /// Cancellation token for this activity. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Activity retry policy
    #[builder(into)]
    pub retry_policy: Option<RetryPolicy>,
    /// Summary of the activity
    pub summary: Option<String>,
    /// Priority for the activity
    pub priority: Option<Priority>,
    /// If true, disable eager execution for this activity
    #[builder(default)]
    pub do_not_eagerly_execute: bool,
    /// Event group markers to attach to the resulting `ScheduleActivityTask` command.
    ///
    /// **Unstable:** Event Groups are not yet implemented in the Rust SDK; this field exists
    /// only for internal test purposes. This API *will* change.
    #[doc(hidden)]
    #[builder(default)]
    pub event_group_markers: Vec<EventGroupMarker>,
}

impl ActivityOptions {
    /// Returns a builder with `close_timeouts` set to [`ActivityCloseTimeouts::StartToClose`].
    pub fn with_start_to_close_timeout(duration: Duration) -> ActivityOptionsBuilder {
        Self::with_close_timeouts(ActivityCloseTimeouts::StartToClose(duration))
    }

    /// Returns a builder with `close_timeouts` set to [`ActivityCloseTimeouts::ScheduleToClose`].
    pub fn with_schedule_to_close_timeout(duration: Duration) -> ActivityOptionsBuilder {
        Self::with_close_timeouts(ActivityCloseTimeouts::ScheduleToClose(duration))
    }

    /// Creates activity options with only `start_to_close_timeout` set.
    ///
    /// If you need additional fields set, use [`Self::with_start_to_close_timeout`].
    pub fn start_to_close_timeout(duration: Duration) -> Self {
        Self::with_start_to_close_timeout(duration).build()
    }

    /// Creates activity options with only `schedule_to_close_timeout` set.
    ///
    /// If you need additional fields set, use [`Self::with_schedule_to_close_timeout`].
    pub fn schedule_to_close_timeout(duration: Duration) -> Self {
        Self::with_schedule_to_close_timeout(duration).build()
    }
}

impl ActivityOptions {
    pub(crate) fn into_command(
        self,
        seq: u32,
        activity_type: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
    ) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                seq,
                activity_type,
                activity_id: self.activity_id.unwrap_or_else(|| seq.to_string()),
                task_queue: self.task_queue.unwrap_or_default(),
                arguments: args,
                headers,
                schedule_to_close_timeout: self
                    .close_timeouts
                    .schedule_to_close()
                    .and_then(|duration| duration.try_into().ok()),
                schedule_to_start_timeout: self
                    .schedule_to_start_timeout
                    .and_then(|duration| duration.try_into().ok()),
                start_to_close_timeout: self
                    .close_timeouts
                    .start_to_close()
                    .and_then(|duration| duration.try_into().ok()),
                heartbeat_timeout: self
                    .heartbeat_timeout
                    .and_then(|duration| duration.try_into().ok()),
                cancellation_type: ProtoActivityCancellationType::from(self.cancellation_type)
                    .into(),
                retry_policy: self.retry_policy.map(Into::into),
                priority: self.priority.map(Into::into),
                do_not_eagerly_execute: self.do_not_eagerly_execute,
                ..Default::default()
            }),
            self.summary,
            None,
            self.event_group_markers,
        )
    }
}

/// Options for scheduling a local activity
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct LocalActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Retry policy
    #[builder(default)]
    pub retry_policy: RetryPolicy,
    /// Override attempt number rather than using 1.
    /// Ideally we would not expose this in a released Rust SDK, but it's needed for test.
    pub attempt: Option<u32>,
    /// Override schedule time when doing timer backoff.
    /// Ideally we would not expose this in a released Rust SDK, but it's needed for test.
    pub original_schedule_time: Option<prost_types::Timestamp>,
    /// Retry backoffs over this amount will use a timer rather than a local retry
    pub timer_backoff_threshold: Option<Duration>,
    /// How the activity will cancel
    #[builder(default)]
    pub cancel_type: ActivityCancellationType,
    /// Cancellation token for this local activity. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Indicates how long the caller is willing to wait for local activity completion. Limits how
    /// long retries will be attempted. When not specified defaults to the workflow execution
    /// timeout (which may be unset).
    pub schedule_to_close_timeout: Option<Duration>,
    /// Limits time the local activity can idle internally before being executed. That can happen if
    /// the worker is currently at max concurrent local activity executions. This timeout is always
    /// non retryable as all a retry would achieve is to put it back into the same queue. Defaults
    /// to `schedule_to_close_timeout` if not specified and that is set. Must be <=
    /// `schedule_to_close_timeout` when set, if not, it will be clamped down.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time the local activity is allowed to execute after the task is dispatched. This
    /// timeout is always retryable. Either or both of `schedule_to_close_timeout` and this must be
    /// specified. If set, this must be <= `schedule_to_close_timeout`, if not, it will be clamped
    /// down.
    pub start_to_close_timeout: Option<Duration>,
    /// Single-line summary for this activity that will appear in UI/CLI.
    pub summary: Option<String>,
    /// Event group markers to attach to the resulting `RecordMarker` command.
    ///
    /// **Unstable:** Event Groups are not yet implemented in the Rust SDK; this field exists
    /// only for internal test purposes. This API *will* change.
    #[doc(hidden)]
    #[builder(default)]
    pub event_group_markers: Vec<EventGroupMarker>,
}

impl Default for LocalActivityOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl LocalActivityOptions {
    pub(crate) fn into_command(
        mut self,
        seq: u32,
        activity_type: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
    ) -> WorkflowCommand {
        // Tests and some workflow code rely on the historical SDK behavior where omitted local
        // activity timeouts are normalized before the command is emitted.
        self.schedule_to_close_timeout
            .get_or_insert(Duration::from_secs(100));
        command_with_metadata(
            workflow_command::Variant::ScheduleLocalActivity(ScheduleLocalActivity {
                seq,
                activity_type,
                activity_id: self.activity_id.unwrap_or_else(|| seq.to_string()),
                arguments: args,
                headers,
                retry_policy: Some(self.retry_policy.into()),
                attempt: self.attempt.unwrap_or(1),
                original_schedule_time: self.original_schedule_time,
                local_retry_threshold: self
                    .timer_backoff_threshold
                    .and_then(|duration| duration.try_into().ok()),
                cancellation_type: ProtoActivityCancellationType::from(self.cancel_type).into(),
                schedule_to_close_timeout: self
                    .schedule_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
                schedule_to_start_timeout: self
                    .schedule_to_start_timeout
                    .and_then(|duration| duration.try_into().ok()),
                start_to_close_timeout: self
                    .start_to_close_timeout
                    .and_then(|duration| duration.try_into().ok()),
            }),
            self.summary,
            None,
            self.event_group_markers,
        )
    }
}

/// Options for scheduling a child workflow
#[derive(Default, Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct ChildWorkflowOptions {
    /// Workflow ID. If unset or empty, the parent workflow generates a deterministic UUIDv4.
    pub workflow_id: Option<String>,
    /// Task queue to schedule the workflow in
    ///
    /// If `None`, use the same task queue as the parent workflow.
    pub task_queue: Option<String>,
    /// Cancellation strategy for the child workflow
    #[builder(default)]
    pub cancel_type: ChildWorkflowCancellationType,
    /// Cancellation token for this child workflow. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// How to respond to parent workflow ending
    #[builder(default)]
    pub parent_close_policy: ParentClosePolicy,
    /// Static summary of the child workflow
    pub static_summary: Option<String>,
    /// Static details of the child workflow
    pub static_details: Option<String>,
    /// Set the policy for reusing the workflow id
    #[builder(default)]
    pub id_reuse_policy: WorkflowIdReusePolicy,
    /// Optionally set the execution timeout for the workflow
    pub execution_timeout: Option<Duration>,
    /// Optionally indicates the default run timeout for a workflow run
    pub run_timeout: Option<Duration>,
    /// Optionally indicates the default task timeout for a workflow run
    pub task_timeout: Option<Duration>,
    /// Optionally set a cron schedule for the workflow
    pub cron_schedule: Option<String>,
    /// Additional search attributes to set on the child workflow.
    pub search_attributes: Option<SearchAttributes>,
    /// Priority for the workflow
    pub priority: Option<Priority>,
    /// Event group markers to attach to the resulting `StartChildWorkflowExecution` command.
    ///
    /// **Unstable:** Event Groups are not yet implemented in the Rust SDK; this field exists
    /// only for internal test purposes. This API *will* change.
    #[doc(hidden)]
    #[builder(default)]
    pub event_group_markers: Vec<EventGroupMarker>,
}

impl ChildWorkflowOptions {
    /// Construct a `ChildWorkflowOptions` with the specified `workflow_id`.
    ///
    /// Shorthand for `ChildWorkflowOptions::builder().workflow_id(Some(workflow_id)).build()`
    pub fn workflow_id(workflow_id: String) -> Self {
        Self::builder().workflow_id(workflow_id).build()
    }

    pub(crate) fn into_command(
        self,
        seq: u32,
        workflow_type: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
        workflow_id: String,
    ) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::StartChildWorkflowExecution(StartChildWorkflowExecution {
                seq,
                workflow_type,
                workflow_id,
                task_queue: self.task_queue.unwrap_or_default(),
                input: args,
                headers,
                cancellation_type: ProtoChildWorkflowCancellationType::from(self.cancel_type)
                    .into(),
                parent_close_policy: ProtoParentClosePolicy::from(self.parent_close_policy).into(),
                workflow_id_reuse_policy: ProtoWorkflowIdReusePolicy::from(
                    match self.id_reuse_policy {
                        WorkflowIdReusePolicy::Unspecified => WorkflowIdReusePolicy::AllowDuplicate,
                        policy => policy,
                    },
                )
                .into(),
                workflow_execution_timeout: self
                    .execution_timeout
                    .and_then(|duration| duration.try_into().ok()),
                workflow_run_timeout: self
                    .run_timeout
                    .and_then(|duration| duration.try_into().ok()),
                workflow_task_timeout: self
                    .task_timeout
                    .and_then(|duration| duration.try_into().ok()),
                cron_schedule: self.cron_schedule.unwrap_or_default(),
                search_attributes: self.search_attributes.map(|t| t.into_proto()),
                priority: self.priority.map(Into::into),
                ..Default::default()
            }),
            self.static_summary,
            self.static_details,
            self.event_group_markers,
        )
    }
}

/// Options for timer
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct TimerOptions {
    /// Duration for the timer
    #[builder(start_fn)]
    pub duration: Duration,
    /// Cancellation token for this timer. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Summary of the timer
    pub summary: Option<String>,
    /// Event group markers to attach to the resulting `StartTimer` command.
    ///
    /// **Unstable:** Event Groups are not yet implemented in the Rust SDK; this field exists
    /// only for internal test purposes. This API *will* change.
    #[doc(hidden)]
    #[builder(default)]
    pub event_group_markers: Vec<EventGroupMarker>,
}

impl Default for TimerOptions {
    fn default() -> Self {
        Self::builder(Duration::default()).build()
    }
}

impl From<Duration> for TimerOptions {
    fn from(duration: Duration) -> Self {
        TimerOptions {
            duration,
            ..Default::default()
        }
    }
}

impl TimerOptions {
    pub(crate) fn into_command(self, seq: u32) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::StartTimer(StartTimer {
                seq,
                start_to_fire_timeout: Some(
                    self.duration
                        .try_into()
                        .expect("workflow timer timeout must fit into protobuf duration"),
                ),
            }),
            self.summary,
            None,
            self.event_group_markers,
        )
    }
}

/// Options for waiting on a workflow condition.
#[derive(Default, Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct WaitConditionOptions {
    /// Cancellation token for this wait. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
}

/// Options for signaling a workflow from another workflow.
#[derive(Default, Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct SignalWorkflowOptions {
    /// Cancellation token for this signal. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Single-line summary for this signal that will appear in UI/CLI.
    pub summary: Option<String>,
    /// Event group markers to attach to the resulting `SignalExternalWorkflowExecution` command.
    ///
    /// **Unstable:** Event Groups are not yet implemented in the Rust SDK; this field exists
    /// only for internal test purposes. This API *will* change.
    #[doc(hidden)]
    #[builder(default)]
    pub event_group_markers: Vec<EventGroupMarker>,
}

impl SignalWorkflowOptions {
    pub(crate) fn into_command(
        self,
        seq: u32,
        signal_name: String,
        args: Vec<Payload>,
        headers: HashMap<String, Payload>,
        target: signal_external_workflow_execution::Target,
    ) -> WorkflowCommand {
        command_with_metadata(
            workflow_command::Variant::SignalExternalWorkflowExecution(
                SignalExternalWorkflowExecution {
                    seq,
                    signal_name,
                    args,
                    target: Some(target),
                    headers,
                },
            ),
            self.summary,
            None,
            self.event_group_markers,
        )
    }
}

/// Options for Nexus Operations
#[derive(Debug, Clone, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct NexusOperationOptions {
    /// Endpoint name, must exist in the endpoint registry or this command will fail.
    pub endpoint: String,
    /// Service name.
    pub service: String,
    /// Operation name.
    pub operation: String,
    /// Input for the operation. The server converts this into Nexus request content and the
    /// appropriate content headers internally when sending the StartOperation request. On the
    /// handler side, if it is also backed by Temporal, the content is transformed back to the
    /// original Payload sent in this command.
    pub input: Option<Payload>,
    /// Schedule-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for operation completion.
    /// Calls are retried internally by the server.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Header to attach to the Nexus request.
    /// Users are responsible for encrypting sensitive data in this header as it is stored in
    /// workflow history and transmitted to external services as-is. This is useful for propagating
    /// tracing information. Note these headers are not the same as Temporal headers on internal
    /// activities and child workflows, these are transmitted to Nexus operations that may be
    /// external and are not traditional payloads.
    #[builder(default)]
    pub nexus_header: HashMap<String, String>,
    /// Cancellation type for the operation
    pub cancellation_type: Option<NexusOperationCancellationType>,
    /// Cancellation token for this operation. `None` inherits workflow cancellation.
    pub cancellation_token: Option<WorkflowCancellationToken>,
    /// Schedule-to-start timeout for this operation.
    /// Indicates how long the caller is willing to wait for the operation to be started (or completed if synchronous)
    /// by the handler. If the operation is not started within this timeout, it will fail with
    /// TIMEOUT_TYPE_SCHEDULE_TO_START.
    /// If not set or zero, no schedule-to-start timeout is enforced.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Start-to-close timeout for this operation.
    /// Indicates how long the caller is willing to wait for an asynchronous operation to complete after it has been
    /// started. If the operation does not complete within this timeout after starting, it will fail with
    /// TIMEOUT_TYPE_START_TO_CLOSE.
    /// Only applies to asynchronous operations. Synchronous operations ignore this timeout.
    /// If not set or zero, no start-to-close timeout is enforced.
    pub start_to_close_timeout: Option<Duration>,
}

impl NexusOperationOptions {
    pub(crate) fn into_command(self, seq: u32) -> WorkflowCommand {
        workflow_command::Variant::ScheduleNexusOperation(ScheduleNexusOperation {
            seq,
            endpoint: self.endpoint,
            service: self.service,
            operation: self.operation,
            input: self.input,
            schedule_to_close_timeout: self
                .schedule_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            schedule_to_start_timeout: self
                .schedule_to_start_timeout
                .and_then(|duration| duration.try_into().ok()),
            start_to_close_timeout: self
                .start_to_close_timeout
                .and_then(|duration| duration.try_into().ok()),
            nexus_header: self.nexus_header,
            cancellation_type: ProtoNexusOperationCancellationType::from(
                self.cancellation_type
                    .unwrap_or(NexusOperationCancellationType::WaitCancellationCompleted),
            )
            .into(),
        })
        .into()
    }
}

/// Versioning behavior to use for the first workflow task of a new continue-as-new run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ContinueAsNewVersioningBehavior {
    /// No initial versioning behavior was specified.
    #[default]
    Unspecified,
    /// Start the new run with AutoUpgrade behavior.
    AutoUpgrade,
    /// Start the new run on the task queue's ramping deployment version.
    UseRampingVersion,
}

impl From<ContinueAsNewVersioningBehavior> for ProtoContinueAsNewVersioningBehavior {
    fn from(value: ContinueAsNewVersioningBehavior) -> Self {
        match value {
            ContinueAsNewVersioningBehavior::Unspecified => {
                ProtoContinueAsNewVersioningBehavior::Unspecified
            }
            ContinueAsNewVersioningBehavior::AutoUpgrade => {
                ProtoContinueAsNewVersioningBehavior::AutoUpgrade
            }
            ContinueAsNewVersioningBehavior::UseRampingVersion => {
                ProtoContinueAsNewVersioningBehavior::UseRampingVersion
            }
        }
    }
}

impl From<ProtoContinueAsNewVersioningBehavior> for ContinueAsNewVersioningBehavior {
    fn from(value: ProtoContinueAsNewVersioningBehavior) -> Self {
        match value {
            ProtoContinueAsNewVersioningBehavior::Unspecified => {
                ContinueAsNewVersioningBehavior::Unspecified
            }
            ProtoContinueAsNewVersioningBehavior::AutoUpgrade => {
                ContinueAsNewVersioningBehavior::AutoUpgrade
            }
            ProtoContinueAsNewVersioningBehavior::UseRampingVersion => {
                ContinueAsNewVersioningBehavior::UseRampingVersion
            }
        }
    }
}

/// Options for continuing a workflow as a new execution.
///
/// Unset fields inherit the current workflow's values where applicable.
#[derive(Default, Debug, bon::Builder)]
#[non_exhaustive]
pub struct ContinueAsNewOptions {
    /// Override the workflow type for the new execution. If `None`, reuses the current type.
    pub workflow_type: Option<String>,
    /// Task queue for the new execution. If `None`, reuses the current task queue.
    pub task_queue: Option<String>,
    /// Timeout for a single run of the new workflow.
    pub run_timeout: Option<Duration>,
    /// Timeout of a single workflow task.
    pub task_timeout: Option<Duration>,
    /// Delay before the first workflow task of the continued run is scheduled.
    pub backoff_start_interval: Option<Duration>,
    /// If set, the new workflow will have these memo values. If `None`, reuses the current memo.
    pub memo: Option<MemoValues>,
    /// If set, the new workflow will have these search attributes. If `None`, reuses the current
    /// search attributes.
    pub search_attributes: Option<SearchAttributes>,
    /// If set, the new workflow will have this retry policy. If `None`, reuses the current policy.
    #[builder(into)]
    pub retry_policy: Option<RetryPolicy>,
    /// Whether the new workflow should run on a worker with a compatible build id.
    pub versioning_intent: Option<VersioningIntent>,
    /// Versioning behavior to use for the first workflow task of the new run.
    ///
    /// This experimental option is only meaningful for workers using worker deployment
    /// versioning. `AutoUpgrade` routes the new run to the current deployment version;
    /// `UseRampingVersion` routes it to the ramping deployment version when one is configured.
    pub initial_versioning_behavior: Option<ContinueAsNewVersioningBehavior>,
}

impl ContinueAsNewOptions {
    pub(crate) fn into_request(
        self,
        workflow_type: String,
        arguments: Vec<Payload>,
        headers: HashMap<String, Payload>,
        payload_converter: &PayloadConverter,
    ) -> Result<ContinueAsNewRequest, PayloadConversionError> {
        let memo = self
            .memo
            .map(|memo| memo.encode(payload_converter))
            .transpose()?
            .unwrap_or_default();
        Ok(ContinueAsNewWorkflowExecution {
            workflow_type: self.workflow_type.unwrap_or(workflow_type),
            task_queue: self.task_queue.unwrap_or_default(),
            arguments,
            workflow_run_timeout: self
                .run_timeout
                .and_then(|duration| duration.try_into().ok()),
            workflow_task_timeout: self
                .task_timeout
                .and_then(|duration| duration.try_into().ok()),
            backoff_start_interval: self
                .backoff_start_interval
                .and_then(|duration| duration.try_into().ok()),
            memo,
            headers,
            search_attributes: self.search_attributes.map(|t| t.into_proto()),
            retry_policy: self.retry_policy.map(Into::into),
            versioning_intent: ProtoVersioningIntent::from(
                self.versioning_intent
                    .unwrap_or(VersioningIntent::Unspecified),
            )
            .into(),
            initial_versioning_behavior: ProtoContinueAsNewVersioningBehavior::from(
                self.initial_versioning_behavior
                    .unwrap_or(ContinueAsNewVersioningBehavior::Unspecified),
            )
            .into(),
        })
    }
}

fn command_with_metadata(
    variant: workflow_command::Variant,
    summary: Option<String>,
    details: Option<String>,
    markers: Vec<EventGroupMarker>,
) -> WorkflowCommand {
    WorkflowCommand {
        variant: Some(variant),
        user_metadata: string_user_metadata(summary, details),
        event_group_markers: markers,
    }
}

fn string_user_metadata(summary: Option<String>, details: Option<String>) -> Option<UserMetadata> {
    if summary.is_none() && details.is_none() {
        return None;
    }
    let converter = PayloadConverter::default();
    let context = SerializationContext {
        data: &SerializationContextData::Workflow,
        converter: &converter,
    };
    Some(UserMetadata {
        summary: summary.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
        details: details.map(|value| {
            converter
                .to_payload(&context, &value)
                .expect("String-to-JSON payload serialization is infallible")
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_cancellation_default_preserves_sdk_behavior() {
        assert_eq!(
            ActivityCancellationType::default(),
            ActivityCancellationType::TryCancel
        );
    }

    #[test]
    fn child_workflow_cancellation_defaults_to_wait_for_completion() {
        assert_eq!(
            ChildWorkflowOptions::default().cancel_type,
            ChildWorkflowCancellationType::WaitCancellationCompleted
        );
        let command = ChildWorkflowOptions::default().into_command(
            1,
            "child".to_string(),
            vec![],
            HashMap::new(),
            "child-id".to_string(),
        );
        let Some(workflow_command::Variant::StartChildWorkflowExecution(command)) = command.variant
        else {
            panic!("expected StartChildWorkflowExecution command");
        };
        assert_eq!(
            command.cancellation_type,
            ProtoChildWorkflowCancellationType::WaitCancellationCompleted as i32
        );
    }

    #[test]
    fn other_policy_defaults_preserve_sdk_behavior() {
        assert_eq!(ParentClosePolicy::default(), ParentClosePolicy::Unspecified);
        assert_eq!(
            WorkflowIdReusePolicy::default(),
            WorkflowIdReusePolicy::Unspecified
        );
        assert_eq!(VersioningIntent::default(), VersioningIntent::Unspecified);
        assert_eq!(
            NexusOperationCancellationType::default(),
            NexusOperationCancellationType::WaitCancellationCompleted
        );
    }

    #[test]
    fn continue_as_new_options_maps_backoff_start_interval_to_request() {
        let req = ContinueAsNewOptions {
            backoff_start_interval: Some(Duration::from_secs(7)),
            versioning_intent: Some(VersioningIntent::Compatible),
            ..Default::default()
        }
        .into_request(
            "test-workflow".to_string(),
            vec![],
            HashMap::new(),
            &PayloadConverter::default(),
        )
        .unwrap();

        let backoff = req
            .backoff_start_interval
            .expect("backoff_start_interval should be set");
        assert_eq!(backoff.seconds, 7);
        assert_eq!(backoff.nanos, 0);
        assert_eq!(
            req.versioning_intent,
            ProtoVersioningIntent::Compatible as i32
        );
    }

    #[test]
    fn activity_options_with_start_to_close_timeout_wrapper_supports_builder_chaining() {
        let opts = ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
            .heartbeat_timeout(Duration::from_secs(2))
            .build();

        assert_eq!(
            opts.close_timeouts,
            ActivityCloseTimeouts::StartToClose(Duration::from_secs(5))
        );
        assert_eq!(opts.heartbeat_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn activity_options_with_schedule_to_close_timeout_wrapper_supports_builder_chaining() {
        let opts = ActivityOptions::with_schedule_to_close_timeout(Duration::from_secs(5))
            .heartbeat_timeout(Duration::from_secs(2))
            .build();

        assert_eq!(
            opts.close_timeouts,
            ActivityCloseTimeouts::ScheduleToClose(Duration::from_secs(5))
        );
        assert_eq!(opts.heartbeat_timeout, Some(Duration::from_secs(2)));
    }

    #[test]
    fn activity_options_both_close_timeouts_map_to_command() {
        let req = ActivityOptions::with_close_timeouts(ActivityCloseTimeouts::Both {
            start_to_close: Duration::from_secs(3),
            schedule_to_close: Duration::from_secs(8),
        })
        .cancellation_type(ActivityCancellationType::Abandon)
        .build()
        .into_command(7, "test".to_string(), vec![], HashMap::new());
        let Some(workflow_command::Variant::ScheduleActivity(req)) = req.variant else {
            panic!("expected ScheduleActivity command");
        };
        assert_eq!(req.start_to_close_timeout.unwrap().seconds, 3);
        assert_eq!(req.schedule_to_close_timeout.unwrap().seconds, 8);
        assert_eq!(
            req.cancellation_type,
            ProtoActivityCancellationType::Abandon as i32
        );
    }

    #[test]
    fn child_workflow_run_timeout_uses_run_timeout_field() {
        let opts = ChildWorkflowOptions {
            workflow_id: Some("test-wf".to_string()),
            cancel_type: ChildWorkflowCancellationType::WaitCancellationRequested,
            parent_close_policy: ParentClosePolicy::RequestCancel,
            id_reuse_policy: WorkflowIdReusePolicy::RejectDuplicate,
            execution_timeout: Some(Duration::from_secs(60)),
            run_timeout: Some(Duration::from_secs(10)),
            ..Default::default()
        };
        let command = opts.into_command(
            1,
            "TestWorkflow".to_string(),
            vec![],
            HashMap::new(),
            "test-wf".into(),
        );
        let Some(workflow_command::Variant::StartChildWorkflowExecution(req)) = command.variant
        else {
            panic!("expected StartChildWorkflowExecution command");
        };
        let exec_timeout = req.workflow_execution_timeout.unwrap();
        let run_timeout = req.workflow_run_timeout.unwrap();
        assert_eq!(exec_timeout.seconds, 60);
        assert_eq!(run_timeout.seconds, 10);
        assert_eq!(
            req.cancellation_type,
            ProtoChildWorkflowCancellationType::WaitCancellationRequested as i32
        );
        assert_eq!(
            req.parent_close_policy,
            ProtoParentClosePolicy::RequestCancel as i32
        );
        assert_eq!(
            req.workflow_id_reuse_policy,
            ProtoWorkflowIdReusePolicy::RejectDuplicate as i32
        );
    }

    #[test]
    fn child_workflow_run_timeout_none_when_unset() {
        let opts = ChildWorkflowOptions {
            workflow_id: Some("test-wf".to_string()),
            execution_timeout: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let command = opts.into_command(
            1,
            "TestWorkflow".to_string(),
            vec![],
            HashMap::new(),
            "test-wf".into(),
        );
        let Some(workflow_command::Variant::StartChildWorkflowExecution(req)) = command.variant
        else {
            panic!("expected StartChildWorkflowExecution command");
        };
        let exec_timeout = req.workflow_execution_timeout.unwrap();
        assert_eq!(exec_timeout.seconds, 60);
        assert!(req.workflow_run_timeout.is_none());
    }
}
