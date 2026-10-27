use crate::Priority;
use std::{
    error::Error,
    marker::PhantomData,
    time::{Duration, SystemTime},
};
use temporalio_common::{
    ActivityDefinition, RetryPolicy, UntypedActivity, WorkerDeploymentVersion,
    data_converters::{
        DataConverter, NoopDecodeHint, PayloadConversionError, SerializationContextData,
        TemporalDeserializable,
    },
    error::IncomingError,
    protos::{
        proto_ts_to_system_time,
        temporal::api::{
            activity::v1::{
                ActivityExecutionInfo as RawInfo, ActivityExecutionListInfo as RawListInfo,
                activity_execution_outcome::Value as ActivityExecutionOutcomeValue,
            },
            common::v1::{Payload, Payloads},
            enums::v1::{
                ActivityExecutionStatus as ProtoActivityExecutionStatus,
                PendingActivityState as ProtoPendingActivityState,
            },
            failure::v1::Failure,
            workflowservice::v1::DescribeActivityExecutionResponse,
        },
        utilities::TryIntoOrNone,
    },
    search_attributes::SearchAttributes,
};

/// Common methods of [`ActivityExecutionInfo`] and [`ActivityExecutionDescription`].
pub trait ActivityExecutionInfoLike {
    /// ID of the activity.
    fn activity_id(&self) -> &str;
    /// Run ID of a particular execution of the activity.
    fn activity_run_id(&self) -> &str;
    /// Type of the activity.
    fn activity_type(&self) -> &str;
    /// Time the activity was originally scheduled.
    fn schedule_time(&self) -> Option<SystemTime>;
    /// Time when the activity transitioned to a closed state.
    fn close_time(&self) -> Option<SystemTime>;
    /// A general status for this activity, indicates whether it is currently running or in one of
    /// the terminal statuses.
    fn status(&self) -> ActivityExecutionStatus;
    /// The task queue this activity was scheduled on.
    fn task_queue(&self) -> &str;
    /// The difference between close time and scheduled time. This field is only populated if
    /// the activity is closed.
    fn execution_duration(&self) -> Option<Duration>;
}

/// Contains basic information about an activity.
/// Obtained from [`Client::list_activities`](crate::Client::list_activities).
pub struct ActivityExecutionInfo {
    raw: RawListInfo,
}

impl From<RawListInfo> for ActivityExecutionInfo {
    fn from(raw: RawListInfo) -> Self {
        Self { raw }
    }
}

impl ActivityExecutionInfoLike for ActivityExecutionInfo {
    fn activity_id(&self) -> &str {
        &self.raw.activity_id
    }

    fn activity_run_id(&self) -> &str {
        &self.raw.run_id
    }

    fn activity_type(&self) -> &str {
        self.raw
            .activity_type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("")
    }

    fn schedule_time(&self) -> Option<SystemTime> {
        self.raw
            .schedule_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    fn close_time(&self) -> Option<SystemTime> {
        self.raw
            .close_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    fn status(&self) -> ActivityExecutionStatus {
        ProtoActivityExecutionStatus::try_from(self.raw.status)
            .map(Into::into)
            .unwrap_or(ActivityExecutionStatus::Unknown)
    }

    fn task_queue(&self) -> &str {
        &self.raw.task_queue
    }

    fn execution_duration(&self) -> Option<Duration> {
        self.raw.execution_duration.try_into_or_none()
    }
}

impl ActivityExecutionInfo {
    /// Raw Protobuf object from server response.
    pub fn raw_info(&self) -> &RawListInfo {
        &self.raw
    }
}

/// Contains the current state of the activity execution.
/// Obtained from [`ActivityHandle::describe`](crate::ActivityHandle::describe).
/// Methods that deserialize payloads (e.g. [`heartbeat_details`](Self::heartbeat_details)) use
/// [`DataConverter`] of the client associated with the activity handle.
pub struct ActivityExecutionDescription<ActivityT = UntypedActivity>
where
    ActivityT: ActivityDefinition,
{
    raw_info: RawInfo,
    raw_input: Option<Payloads>,
    raw_outcome: Option<ActivityExecutionOutcomeValue>,
    data_converter: DataConverter,
    serialization_context: SerializationContextData,
    _phantom: PhantomData<ActivityT>,
}

impl<ActivityT> ActivityExecutionInfoLike for ActivityExecutionDescription<ActivityT>
where
    ActivityT: ActivityDefinition,
{
    fn activity_id(&self) -> &str {
        &self.raw_info.activity_id
    }

    fn activity_run_id(&self) -> &str {
        &self.raw_info.run_id
    }

    fn activity_type(&self) -> &str {
        self.raw_info
            .activity_type
            .as_ref()
            .map(|t| t.name.as_str())
            .unwrap_or("")
    }

    fn schedule_time(&self) -> Option<SystemTime> {
        self.raw_info
            .schedule_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    fn close_time(&self) -> Option<SystemTime> {
        self.raw_info
            .close_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    fn status(&self) -> ActivityExecutionStatus {
        ProtoActivityExecutionStatus::try_from(self.raw_info.status)
            .map(Into::into)
            .unwrap_or(ActivityExecutionStatus::Unknown)
    }

    fn task_queue(&self) -> &str {
        &self.raw_info.task_queue
    }

    fn execution_duration(&self) -> Option<Duration> {
        self.raw_info.execution_duration.try_into_or_none()
    }
}

impl<ActivityT> ActivityExecutionDescription<ActivityT>
where
    ActivityT: ActivityDefinition,
{
    pub(crate) fn new(
        data_converter: DataConverter,
        serialization_context: SerializationContextData,
        response: DescribeActivityExecutionResponse,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let Some(raw_info) = response.info else {
            return Err("info missing in describe response".into());
        };
        Ok(Self {
            raw_info,
            raw_input: response.input,
            raw_outcome: response.outcome.and_then(|o| o.value),
            data_converter,
            serialization_context,
            _phantom: PhantomData,
        })
    }

    /// Convert to an untyped description object.
    pub fn untyped(self) -> ActivityExecutionDescription {
        ActivityExecutionDescription {
            raw_info: self.raw_info,
            raw_input: self.raw_input,
            raw_outcome: self.raw_outcome,
            data_converter: self.data_converter,
            serialization_context: self.serialization_context,
            _phantom: PhantomData,
        }
    }

    /// Raw Protobuf object from server response.
    pub fn raw_info(&self) -> &RawInfo {
        &self.raw_info
    }

    /// True if activity input is present.
    /// See [`ActivityDescribeOptions::include_input`](crate::ActivityDescribeOptions::include_input).
    /// Use [`input`](Self::input) or [`raw_input`](Self::raw_input) to retrieve it.
    pub fn has_input(&self) -> bool {
        self.raw_input.is_some()
    }

    /// Raw payload of activity input, if it was requested.
    pub fn raw_input(&self) -> Option<&Payloads> {
        self.raw_input.as_ref()
    }

    /// Deserialize activity input. Returns `Ok(None)` if not present.
    /// See [`ActivityDescribeOptions::include_input`](crate::ActivityDescribeOptions::include_input).
    pub async fn input(&self) -> Result<Option<ActivityT::Input>, PayloadConversionError> {
        let Some(input) = &self.raw_input else {
            return Ok(None);
        };
        Ok(Some(self.convert_payloads(input).await?))
    }

    /// True if activity outcome is present.
    /// See [`ActivityDescribeOptions::include_outcome`](crate::ActivityDescribeOptions::include_outcome).
    /// Use [`outcome`](Self::outcome) or [`raw_outcome`](Self::outcome) to retrieve it.
    pub fn has_outcome(&self) -> bool {
        self.raw_outcome.is_some()
    }

    /// Raw payload of activity output, if it was requested and available.
    pub fn raw_outcome(&self) -> Option<&ActivityExecutionOutcomeValue> {
        self.raw_outcome.as_ref()
    }

    /// Deserialize activity outcome. Returns `Ok(None)` if not present.
    /// See [`ActivityDescribeOptions::include_outcome`](crate::ActivityDescribeOptions::include_outcome).
    pub async fn outcome(
        &self,
    ) -> Result<Option<Result<ActivityT::Output, IncomingError>>, PayloadConversionError> {
        match &self.raw_outcome {
            None => Ok(None),
            Some(ActivityExecutionOutcomeValue::Result(payloads)) => {
                Ok(Some(Ok(self.convert_payloads(payloads).await?)))
            }
            Some(ActivityExecutionOutcomeValue::Failure(failure)) => {
                Ok(Some(Err(self.convert_failure(failure)?)))
            }
        }
    }

    /// More detailed breakdown of [`ActivityExecutionStatus::Running`].
    pub fn run_state(&self) -> PendingActivityState {
        ProtoPendingActivityState::try_from(self.raw_info.run_state)
            .map(Into::into)
            .unwrap_or(PendingActivityState::Unknown)
    }

    /// Indicates how long the caller is willing to wait for an activity completion. Limits how long
    /// retries will be attempted.
    pub fn schedule_to_close_timeout(&self) -> Option<Duration> {
        self.raw_info.schedule_to_close_timeout.try_into_or_none()
    }

    /// Limits time an activity task can stay in a task queue before a worker picks it up. This
    /// timeout is always non-retryable.
    pub fn schedule_to_start_timeout(&self) -> Option<Duration> {
        self.raw_info.schedule_to_start_timeout.try_into_or_none()
    }

    /// Maximum time a single activity attempt is allowed to execute after being picked up by
    /// a worker. This timeout is always retryable.
    pub fn start_to_close_timeout(&self) -> Option<Duration> {
        self.raw_info.start_to_close_timeout.try_into_or_none()
    }

    /// Maximum permitted time between successful worker heartbeats.
    pub fn heartbeat_timeout(&self) -> Option<Duration> {
        self.raw_info.heartbeat_timeout.try_into_or_none()
    }

    /// The retry policy for the activity.
    pub fn retry_policy(&self) -> Option<RetryPolicy> {
        self.raw_info.retry_policy.clone().map(Into::into)
    }

    /// True if heartbeat details are present.
    /// See [`ActivityDescribeOptions::include_heartbeat_details`](crate::ActivityDescribeOptions::include_heartbeat_details).
    /// Use [`heartbeat_details`](Self::heartbeat_details) or
    /// [`raw_info()`](Self::raw_info)`.`[`heartbeat_details`](RawInfo::heartbeat_details)
    /// to retrieve them.
    pub fn has_heartbeat_details(&self) -> bool {
        self.raw_info.heartbeat_details.is_some()
    }

    /// Deserialize heartbeat details. Returns `Ok(None)` if not present.
    /// See [`ActivityDescribeOptions::include_heartbeat_details`](crate::ActivityDescribeOptions::include_heartbeat_details).
    pub async fn heartbeat_details<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        let Some(details) = &self.raw_info.heartbeat_details else {
            return Ok(None);
        };
        Ok(Some(self.convert_payloads(details).await?))
    }

    /// Time the last heartbeat was recorded.
    pub fn last_heartbeat_time(&self) -> Option<SystemTime> {
        self.raw_info
            .last_heartbeat_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// Time the last attempt was started.
    pub fn last_started_time(&self) -> Option<SystemTime> {
        self.raw_info
            .last_started_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// The attempt this activity is currently on. Incremented each time a new attempt is scheduled.
    pub fn attempt(&self) -> u32 {
        self.raw_info.attempt.try_into().unwrap_or_default()
    }

    /// How long this activity has been running for, including all attempts and backoff between
    /// attempts.
    pub fn execution_duration(&self) -> Option<Duration> {
        self.raw_info.execution_duration.try_into_or_none()
    }

    /// Scheduled time + schedule to close timeout.
    pub fn expiration_time(&self) -> Option<SystemTime> {
        self.raw_info
            .expiration_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// True if last failure is present.
    /// See [`ActivityDescribeOptions::include_last_failure`](crate::ActivityDescribeOptions::include_last_failure).
    /// Use [`last_failure()`](Self::last_failure) or
    /// [`raw_info()`](Self::raw_info)`.`[`last_failure`](RawInfo::last_failure)
    /// to retrieve it.
    pub fn has_last_failure(&self) -> bool {
        self.raw_info.last_failure.is_some()
    }

    /// Deserialize last failure. Returns `Ok(None)` if not present.
    /// See [`ActivityDescribeOptions::include_last_failure`](crate::ActivityDescribeOptions::include_last_failure).
    pub fn last_failure(&self) -> Result<Option<IncomingError>, PayloadConversionError> {
        let Some(failure) = &self.raw_info.last_failure else {
            return Ok(None);
        };
        Ok(Some(self.convert_failure(failure)?))
    }

    /// Identity of the last worker that attempted this activity.
    pub fn last_worker_identity(&self) -> Option<&str> {
        self.raw_info
            .last_worker_identity
            .is_empty()
            .then_some(self.raw_info.last_worker_identity.as_str())
    }

    /// Time from the last attempt failure to the next activity retry.
    pub fn current_retry_interval(&self) -> Option<Duration> {
        self.raw_info.current_retry_interval.try_into_or_none()
    }

    /// The time when the last activity attempt completed.
    pub fn last_attempt_complete_time(&self) -> Option<SystemTime> {
        self.raw_info
            .last_attempt_complete_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// The time when the next activity attempt will be scheduled.
    pub fn next_attempt_schedule_time(&self) -> Option<SystemTime> {
        self.raw_info
            .next_attempt_schedule_time
            .as_ref()
            .and_then(proto_ts_to_system_time)
    }

    /// The Worker Deployment Version this activity was dispatched to most recently.
    pub fn last_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        self.raw_info
            .last_deployment_version
            .clone()
            .map(Into::into)
    }

    /// Priority metadata.
    pub fn priority(&self) -> Priority {
        self.raw_info.priority.clone().unwrap_or_default().into()
    }

    /// Search attributes of the activity.
    pub fn search_attributes(&self) -> Option<SearchAttributes> {
        self.raw_info
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
    }

    /// Deserialize static summary that was set when activity was scheduled.
    /// Returns `Ok(None)` if not present.
    pub async fn static_summary(&self) -> Result<Option<String>, PayloadConversionError> {
        let Some(summary) = self
            .raw_info
            .user_metadata
            .as_ref()
            .and_then(|m| m.summary.clone())
        else {
            return Ok(None);
        };
        Ok(Some(self.convert_payload(summary).await?))
    }

    /// Deserialize static details that were set when activity was scheduled.
    /// Returns `Ok(None)` if not present.
    pub async fn static_details(&self) -> Result<Option<String>, PayloadConversionError> {
        let Some(details) = self
            .raw_info
            .user_metadata
            .as_ref()
            .and_then(|m| m.details.clone())
        else {
            return Ok(None);
        };
        Ok(Some(self.convert_payload(details).await?))
    }

    /// Reason for activity cancellation if activity was canceled and reason was provided.
    pub fn canceled_reason(&self) -> Option<&str> {
        let reason = self.raw_info.canceled_reason.as_str();
        (!reason.is_empty()).then_some(reason)
    }

    /// Time to wait before dispatching the first activity task.
    /// This delay is not applied to retry attempts.
    pub fn start_delay(&self) -> Option<Duration> {
        self.raw_info.start_delay.try_into_or_none()
    }

    async fn convert_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        self.data_converter
            .from_payload(&self.serialization_context, payload)
            .await
    }

    async fn convert_payloads<T: TemporalDeserializable + 'static>(
        &self,
        payloads: &Payloads,
    ) -> Result<T, PayloadConversionError> {
        self.data_converter
            .from_payloads(&self.serialization_context, payloads.payloads.clone())
            .await
    }

    fn convert_failure(&self, failure: &Failure) -> Result<IncomingError, PayloadConversionError> {
        self.data_converter
            .to_error(&self.serialization_context, failure.clone(), NoopDecodeHint)
    }
}

/// Execution status of an activity. See [`ActivityExecutionInfoLike::status`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ActivityExecutionStatus {
    #[default]
    /// This variant indicates the server did not specify a value.
    Unspecified,
    /// The activity has not reached a terminal status.
    /// See [`ActivityExecutionDescription::run_state`] for the run state.
    Running,
    /// The activity completed successfully.
    Completed,
    /// The activity failed with an error.
    Failed,
    /// The activity was canceled. Note that cancellation is cooperative and a cancel request does
    /// not always result in canceled status.
    Canceled,
    /// The activity was terminated.
    Terminated,
    /// The activity timed out.
    TimedOut,
    /// The activity is paused.
    Paused,
    /// This variant indicates the server used a value not known by this version of the SDK.
    Unknown,
}

impl From<ProtoActivityExecutionStatus> for ActivityExecutionStatus {
    fn from(value: ProtoActivityExecutionStatus) -> Self {
        match value {
            ProtoActivityExecutionStatus::Unspecified => Self::Unspecified,
            ProtoActivityExecutionStatus::Running => Self::Running,
            ProtoActivityExecutionStatus::Completed => Self::Completed,
            ProtoActivityExecutionStatus::Failed => Self::Failed,
            ProtoActivityExecutionStatus::Canceled => Self::Canceled,
            ProtoActivityExecutionStatus::Terminated => Self::Terminated,
            ProtoActivityExecutionStatus::TimedOut => Self::TimedOut,
            ProtoActivityExecutionStatus::Paused => Self::Paused,
        }
    }
}

/// Detailed state of an activity with [`ActivityExecutionStatus::Running`].
/// See [`ActivityExecutionDescription::run_state`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PendingActivityState {
    #[default]
    /// This variant indicates the server did not specify a state.
    Unspecified,
    /// Activity is scheduled for execution but not yet running on a worker.
    Scheduled,
    /// Activity is running on a worker.
    Started,
    /// Activity has been requested to cancel.
    CancelRequested,
    /// Activity is paused on the server, and is not running on a worker.
    Paused,
    /// Activity is currently running on a worker, but paused on the server.
    PauseRequested,
    /// This variant indicates the server used a value not known by this version of the SDK.
    Unknown,
}

impl From<ProtoPendingActivityState> for PendingActivityState {
    fn from(value: ProtoPendingActivityState) -> Self {
        match value {
            ProtoPendingActivityState::Unspecified => Self::Unspecified,
            ProtoPendingActivityState::Scheduled => Self::Scheduled,
            ProtoPendingActivityState::Started => Self::Started,
            ProtoPendingActivityState::CancelRequested => Self::CancelRequested,
            ProtoPendingActivityState::Paused => Self::Paused,
            ProtoPendingActivityState::PauseRequested => Self::PauseRequested,
        }
    }
}
