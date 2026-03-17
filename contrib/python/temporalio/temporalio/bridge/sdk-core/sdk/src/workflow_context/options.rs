use std::{collections::HashMap, time::Duration};

use temporal_client::{Priority, WorkflowOptions};
use temporal_sdk_core_protos::{
    coresdk::{
        child_workflow::ChildWorkflowCancellationType,
        nexus::NexusOperationCancellationType,
        workflow_commands::{
            ActivityCancellationType, ScheduleActivity, ScheduleLocalActivity,
            ScheduleNexusOperation, StartChildWorkflowExecution, WorkflowCommand,
        },
    },
    temporal::api::{
        common::v1::{Payload, RetryPolicy},
        enums::v1::ParentClosePolicy,
        sdk::v1::UserMetadata,
    },
};
// TODO: Before release, probably best to avoid using proto types entirely here. They're awkward.

pub(crate) trait IntoWorkflowCommand {
    /// Produces a workflow command from some options
    fn into_command(self, seq: u32) -> WorkflowCommand;
}

/// Options for scheduling an activity
#[derive(Default, Debug)]
pub struct ActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Type of activity to schedule
    pub activity_type: String,
    /// Input to the activity
    pub input: Payload,
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
    /// Maximum time of a single Activity execution attempt.
    /// Note that the Temporal Server doesn't detect Worker process failures directly.
    /// It relies on this timeout to detect that an Activity that didn't complete on time.
    /// So this timeout should be as short as the longest possible execution of the Activity body.
    /// Potentially long running Activities must specify `heartbeat_timeout` and heartbeat from the
    /// activity periodically for timely failure detection.
    /// Either this option or `schedule_to_close_timeout` is required.
    pub start_to_close_timeout: Option<Duration>,
    /// Total time that a workflow is willing to wait for Activity to complete.
    /// `schedule_to_close_timeout` limits the total time of an Activity's execution including
    /// retries (use `start_to_close_timeout` to limit the time of a single attempt).
    /// Either this option or `start_to_close_timeout` is required.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Heartbeat interval. Activity must heartbeat before this interval passes after a last
    /// heartbeat or activity start.
    pub heartbeat_timeout: Option<Duration>,
    /// Determines what the SDK does when the Activity is cancelled.
    pub cancellation_type: ActivityCancellationType,
    /// Activity retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Summary of the activity
    pub summary: Option<String>,
    /// Priority for the activity
    pub priority: Option<Priority>,
    /// If true, disable eager execution for this activity
    pub do_not_eagerly_execute: bool,
}

impl IntoWorkflowCommand for ActivityOptions {
    fn into_command(self, seq: u32) -> WorkflowCommand {
        WorkflowCommand {
            variant: Some(
                ScheduleActivity {
                    seq,
                    activity_id: match self.activity_id {
                        None => seq.to_string(),
                        Some(aid) => aid,
                    },
                    activity_type: self.activity_type,
                    task_queue: self.task_queue.unwrap_or_default(),
                    schedule_to_close_timeout: self
                        .schedule_to_close_timeout
                        .and_then(|d| d.try_into().ok()),
                    schedule_to_start_timeout: self
                        .schedule_to_start_timeout
                        .and_then(|d| d.try_into().ok()),
                    start_to_close_timeout: self
                        .start_to_close_timeout
                        .and_then(|d| d.try_into().ok()),
                    heartbeat_timeout: self.heartbeat_timeout.and_then(|d| d.try_into().ok()),
                    cancellation_type: self.cancellation_type as i32,
                    arguments: vec![self.input],
                    retry_policy: self.retry_policy,
                    priority: self.priority.map(Into::into),
                    do_not_eagerly_execute: self.do_not_eagerly_execute,
                    ..Default::default()
                }
                .into(),
            ),
            user_metadata: self.summary.map(|s| UserMetadata {
                summary: Some(s.into()),
                details: None,
            }),
        }
    }
}

/// Options for scheduling a local activity
#[derive(Default, Debug, Clone)]
pub struct LocalActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Type of activity to schedule
    pub activity_type: String,
    /// Input to the activity
    // TODO: Make optional
    pub input: Payload,
    /// Retry policy
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
    pub cancel_type: ActivityCancellationType,
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
}

impl IntoWorkflowCommand for LocalActivityOptions {
    fn into_command(mut self, seq: u32) -> WorkflowCommand {
        // Allow tests to avoid extra verbosity when they don't care about timeouts
        // TODO: Builderize LA options
        self.schedule_to_close_timeout
            .get_or_insert(Duration::from_secs(100));

        WorkflowCommand {
            variant: Some(
                ScheduleLocalActivity {
                    seq,
                    attempt: self.attempt.unwrap_or(1),
                    original_schedule_time: self.original_schedule_time,
                    activity_id: match self.activity_id {
                        None => seq.to_string(),
                        Some(aid) => aid,
                    },
                    activity_type: self.activity_type,
                    arguments: vec![self.input],
                    retry_policy: Some(self.retry_policy),
                    local_retry_threshold: self
                        .timer_backoff_threshold
                        .and_then(|d| d.try_into().ok()),
                    cancellation_type: self.cancel_type.into(),
                    schedule_to_close_timeout: self
                        .schedule_to_close_timeout
                        .and_then(|d| d.try_into().ok()),
                    schedule_to_start_timeout: self
                        .schedule_to_start_timeout
                        .and_then(|d| d.try_into().ok()),
                    start_to_close_timeout: self
                        .start_to_close_timeout
                        .and_then(|d| d.try_into().ok()),
                    ..Default::default()
                }
                .into(),
            ),
            user_metadata: None,
        }
    }
}

/// Options for scheduling a child workflow
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflowOptions {
    /// Workflow ID
    pub workflow_id: String,
    /// Type of workflow to schedule
    pub workflow_type: String,
    /// Task queue to schedule the workflow in
    ///
    /// If `None`, use the same task queue as the parent workflow.
    pub task_queue: Option<String>,
    /// Input to send the child Workflow
    pub input: Vec<Payload>,
    /// Cancellation strategy for the child workflow
    pub cancel_type: ChildWorkflowCancellationType,
    /// Common options
    pub options: WorkflowOptions,
    /// How to respond to parent workflow ending
    pub parent_close_policy: ParentClosePolicy,
    /// Static summary of the child workflow
    pub static_summary: Option<String>,
    /// Static details of the child workflow
    pub static_details: Option<String>,
}

impl IntoWorkflowCommand for ChildWorkflowOptions {
    fn into_command(self, seq: u32) -> WorkflowCommand {
        let user_metadata = if self.static_summary.is_some() || self.static_details.is_some() {
            Some(UserMetadata {
                summary: self.static_summary.map(Into::into),
                details: self.static_details.map(Into::into),
            })
        } else {
            None
        };
        WorkflowCommand {
            variant: Some(
                StartChildWorkflowExecution {
                    seq,
                    workflow_id: self.workflow_id,
                    workflow_type: self.workflow_type,
                    task_queue: self.task_queue.unwrap_or_default(),
                    input: self.input,
                    cancellation_type: self.cancel_type as i32,
                    workflow_id_reuse_policy: self.options.id_reuse_policy as i32,
                    workflow_execution_timeout: self
                        .options
                        .execution_timeout
                        .and_then(|d| d.try_into().ok()),
                    workflow_run_timeout: self
                        .options
                        .execution_timeout
                        .and_then(|d| d.try_into().ok()),
                    workflow_task_timeout: self
                        .options
                        .task_timeout
                        .and_then(|d| d.try_into().ok()),
                    search_attributes: self.options.search_attributes.unwrap_or_default(),
                    cron_schedule: self.options.cron_schedule.unwrap_or_default(),
                    parent_close_policy: self.parent_close_policy as i32,
                    priority: self.options.priority.map(Into::into),
                    ..Default::default()
                }
                .into(),
            ),
            user_metadata,
        }
    }
}

/// Options for sending a signal to an external workflow
pub struct SignalWorkflowOptions {
    /// The workflow's id
    pub workflow_id: String,
    /// The particular run to target, or latest if `None`
    pub run_id: Option<String>,
    /// The details of the signal to send
    pub signal: Signal,
}

impl SignalWorkflowOptions {
    /// Create options for sending a signal to another workflow
    pub fn new(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        name: impl Into<String>,
        input: impl IntoIterator<Item = impl Into<Payload>>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: Some(run_id.into()),
            signal: Signal::new(name, input),
        }
    }

    /// Set a header k/v pair attached to the signal
    pub fn with_header(
        &mut self,
        key: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> &mut Self {
        self.signal.data.with_header(key.into(), payload.into());
        self
    }
}

/// Information needed to send a specific signal
pub struct Signal {
    /// The signal name
    pub signal_name: String,
    /// The data the signal carries
    pub data: SignalData,
}

impl Signal {
    /// Create a new signal
    pub fn new(
        name: impl Into<String>,
        input: impl IntoIterator<Item = impl Into<Payload>>,
    ) -> Self {
        Self {
            signal_name: name.into(),
            data: SignalData::new(input),
        }
    }
}

/// Data contained within a signal
#[derive(Default, Debug)]
pub struct SignalData {
    /// The arguments the signal will receive
    pub input: Vec<Payload>,
    /// Metadata attached to the signal
    pub headers: HashMap<String, Payload>,
}

impl SignalData {
    /// Create data for a signal
    pub fn new(input: impl IntoIterator<Item = impl Into<Payload>>) -> Self {
        Self {
            input: input.into_iter().map(Into::into).collect(),
            headers: HashMap::new(),
        }
    }

    /// Set a header k/v pair attached to the signal
    pub fn with_header(
        &mut self,
        key: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> &mut Self {
        self.headers.insert(key.into(), payload.into());
        self
    }
}

/// Options for timer
#[derive(Default, Debug, Clone)]
pub struct TimerOptions {
    /// Duration for the timer
    pub duration: Duration,
    /// Summary of the timer
    pub summary: Option<String>,
}

impl From<Duration> for TimerOptions {
    fn from(duration: Duration) -> Self {
        TimerOptions {
            duration,
            ..Default::default()
        }
    }
}

/// Options for Nexus Operations
#[derive(Default, Debug, Clone)]
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
    pub nexus_header: HashMap<String, String>,
    /// Cancellation type for the operation
    pub cancellation_type: Option<NexusOperationCancellationType>,
}

impl IntoWorkflowCommand for NexusOperationOptions {
    fn into_command(self, seq: u32) -> WorkflowCommand {
        WorkflowCommand {
            user_metadata: None,
            variant: Some(
                ScheduleNexusOperation {
                    seq,
                    endpoint: self.endpoint,
                    service: self.service,
                    operation: self.operation,
                    input: self.input,
                    schedule_to_close_timeout: self
                        .schedule_to_close_timeout
                        .and_then(|t| t.try_into().ok()),
                    nexus_header: self.nexus_header,
                    cancellation_type: self
                        .cancellation_type
                        .unwrap_or(NexusOperationCancellationType::WaitCancellationCompleted)
                        .into(),
                }
                .into(),
            ),
        }
    }
}
