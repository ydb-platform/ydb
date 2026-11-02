use crate::{
    BackfillScheduleInput, Client, CreateScheduleInput, CreateScheduleOutput, DeleteScheduleInput,
    DescribeScheduleInput, DescribeScheduleOutput, ListSchedulesPageInput, ListSchedulesPageOutput,
    NamespacedClient, Next, PauseScheduleInput, RpcOptions, SendScheduleUpdateInput,
    TriggerScheduleInput, UnpauseScheduleInput, UpdateScheduleInput, grpc::WorkflowService,
    interceptors,
};
use futures_util::{FutureExt, future::BoxFuture, stream};
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use temporalio_common::{
    HasWorkflowDefinition,
    data_converters::{
        DataConverter, PayloadConversionError, SerializationContextData, TemporalDeserializable,
        TemporalSerializable,
    },
    payload_visitor::decode_payloads,
    protos::{
        coresdk::IntoPayloadsExt,
        proto_ts_to_system_time,
        temporal::api::{
            common::v1 as common_proto, schedule::v1 as schedule_proto,
            taskqueue::v1 as taskqueue_proto, workflow::v1 as workflow_proto,
            workflowservice::v1::*,
        },
    },
    search_attributes::SearchAttributes,
};
use tonic::IntoRequest;
use uuid::Uuid;

/// Errors returned by schedule operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ScheduleError {
    /// An rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
    /// Failed to encode workflow input payloads.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// The server returned a schedule description that is missing required fields.
    #[error("Malformed schedule description for schedule ID '{schedule_id}': {reason}")]
    MalformedDescription {
        /// ID of the schedule whose description was malformed.
        schedule_id: String,
        /// Details about the malformed response.
        reason: String,
    },
}

trait SerializableScheduleInput: Send + Sync {
    fn to_payloads<'a>(
        &'a self,
        dc: &'a DataConverter,
        context: &'a SerializationContextData,
    ) -> BoxFuture<'a, Result<Vec<common_proto::Payload>, PayloadConversionError>>;
}

impl<T> SerializableScheduleInput for T
where
    T: TemporalSerializable + Send + Sync + 'static,
{
    fn to_payloads<'a>(
        &'a self,
        dc: &'a DataConverter,
        context: &'a SerializationContextData,
    ) -> BoxFuture<'a, Result<Vec<common_proto::Payload>, PayloadConversionError>> {
        dc.to_payloads(context, self).boxed()
    }
}

/// Workflow input for a schedule action, stored unencoded until the schedule is created.
#[derive(derive_more::Debug, Clone)]
pub struct ScheduleWorkflowInput {
    repr: ScheduleWorkflowInputRepr,
}

#[derive(derive_more::Debug, Clone)]
enum ScheduleWorkflowInputRepr {
    #[debug("Deferred(...)")]
    Deferred(#[debug(skip)] Arc<dyn SerializableScheduleInput>),
}

impl ScheduleWorkflowInput {
    fn new_deferred<T>(val: T) -> Self
    where
        T: SerializableScheduleInput + 'static,
    {
        Self {
            repr: ScheduleWorkflowInputRepr::Deferred(Arc::new(val)),
        }
    }

    pub(crate) async fn into_payloads(
        self,
        dc: &DataConverter,
    ) -> Result<Vec<common_proto::Payload>, PayloadConversionError> {
        let ScheduleWorkflowInputRepr::Deferred(v) = self.repr;
        v.to_payloads(dc, &SerializationContextData::Workflow).await
    }
}

/// Options for creating a schedule.
#[derive(Debug, Clone, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct CreateScheduleOptions {
    /// The action the schedule should perform on each trigger.
    pub action: ScheduleAction,
    /// Defines when the schedule should trigger.
    pub spec: ScheduleSpec,
    /// Whether to trigger the schedule immediately upon creation.
    #[builder(default)]
    pub trigger_immediately: bool,
    /// Overlap policy for the schedule. Also used for the initial trigger when
    /// `trigger_immediately` is true.
    #[builder(default)]
    pub overlap_policy: ScheduleOverlapPolicy,
    /// Whether the schedule starts in a paused state.
    #[builder(default)]
    pub paused: bool,
    /// A note to attach to the schedule state (e.g., reason for pausing).
    #[builder(default)]
    pub note: String,
    /// Controls for the create RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// The action a schedule should perform on each trigger.
// TODO: The proto supports other action types beyond StartWorkflow.
#[derive(derive_more::Debug, Clone)]
#[non_exhaustive]
pub enum ScheduleAction {
    /// Start a workflow execution.
    StartWorkflow {
        /// The workflow type name.
        workflow_type: String,
        /// The task queue to run the workflow on.
        task_queue: String,
        /// The workflow ID prefix. The server may append a timestamp.
        workflow_id: String,
        /// Workflow input to pass on each execution. `None` means no input.
        input: Option<ScheduleWorkflowInput>,
    },
}

impl ScheduleAction {
    /// Create a start-workflow action. Input is encoded when the schedule is created.
    pub fn start_workflow<W>(
        workflow: W,
        input: W::Input,
        task_queue: impl Into<String>,
        workflow_id: impl Into<String>,
    ) -> Self
    where
        W: HasWorkflowDefinition,
        W::Input: TemporalSerializable + Send + Sync + 'static,
    {
        Self::StartWorkflow {
            workflow_type: workflow.name().to_string(),
            task_queue: task_queue.into(),
            workflow_id: workflow_id.into(),
            input: Some(ScheduleWorkflowInput::new_deferred(input)),
        }
    }

    pub(crate) async fn into_proto(
        self,
        dc: &DataConverter,
    ) -> Result<schedule_proto::ScheduleAction, PayloadConversionError> {
        match self {
            Self::StartWorkflow {
                workflow_type,
                task_queue,
                workflow_id,
                input,
            } => {
                let input = if let Some(wi) = input {
                    wi.into_payloads(dc).await?.into_payloads()
                } else {
                    None
                };
                Ok(schedule_proto::ScheduleAction {
                    action: Some(schedule_proto::schedule_action::Action::StartWorkflow(
                        workflow_proto::NewWorkflowExecutionInfo {
                            workflow_id,
                            workflow_type: Some(common_proto::WorkflowType {
                                name: workflow_type,
                            }),
                            task_queue: Some(taskqueue_proto::TaskQueue {
                                name: task_queue,
                                ..Default::default()
                            }),
                            input,
                            ..Default::default()
                        },
                    )),
                })
            }
        }
    }
}

/// Defines when a schedule should trigger.
///
/// Note: `set_spec` on [`ScheduleUpdate`] replaces the entire spec. Fields not
/// set here will use their proto defaults on the server.
#[derive(Debug, Clone, Default, PartialEq, bon::Builder)]
#[builder(on(String, into))]
pub struct ScheduleSpec {
    /// Interval-based triggers (e.g., every 1 hour).
    #[builder(default)]
    pub intervals: Vec<ScheduleIntervalSpec>,
    /// Calendar-based triggers using range strings.
    #[builder(default)]
    pub calendars: Vec<ScheduleCalendarSpec>,
    /// Calendar-based exclusions. Matching times are skipped.
    #[builder(default)]
    pub exclude_calendars: Vec<ScheduleCalendarSpec>,
    /// Cron expression triggers (e.g., `"0 12 * * MON-FRI"`).
    #[builder(default)]
    pub cron_strings: Vec<String>,
    /// IANA timezone name (e.g., `"US/Eastern"`). Empty uses UTC.
    #[builder(default)]
    pub timezone_name: String,
    /// Earliest time the schedule is active.
    pub start_time: Option<SystemTime>,
    /// Latest time the schedule is active.
    pub end_time: Option<SystemTime>,
    /// Random jitter applied to each action time.
    pub jitter: Option<Duration>,
}

impl ScheduleSpec {
    /// Create a spec that triggers on a single interval.
    pub fn from_interval(every: Duration) -> Self {
        Self {
            intervals: vec![every.into()],
            ..Default::default()
        }
    }

    /// Create a spec that triggers on a single calendar schedule.
    pub fn from_calendar(calendar: ScheduleCalendarSpec) -> Self {
        Self {
            calendars: vec![calendar],
            ..Default::default()
        }
    }

    pub(crate) fn into_proto(self) -> schedule_proto::ScheduleSpec {
        #[allow(deprecated)]
        schedule_proto::ScheduleSpec {
            interval: self.intervals.into_iter().map(Into::into).collect(),
            calendar: self.calendars.into_iter().map(Into::into).collect(),
            exclude_calendar: self.exclude_calendars.into_iter().map(Into::into).collect(),
            cron_string: self.cron_strings,
            timezone_name: self.timezone_name,
            start_time: self.start_time.map(Into::into),
            end_time: self.end_time.map(Into::into),
            jitter: self.jitter.and_then(|d| d.try_into().ok()),
            ..Default::default()
        }
    }
}

/// An interval-based schedule trigger.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ScheduleIntervalSpec {
    /// How often the action should repeat.
    pub every: Duration,
    /// Fixed offset added to each interval.
    pub offset: Option<Duration>,
}

impl ScheduleIntervalSpec {
    /// Create an interval with an optional offset.
    pub fn new(every: Duration, offset: Option<Duration>) -> Self {
        Self { every, offset }
    }
}

impl From<Duration> for ScheduleIntervalSpec {
    fn from(every: Duration) -> Self {
        Self {
            every,
            offset: None,
        }
    }
}

impl From<ScheduleIntervalSpec> for schedule_proto::IntervalSpec {
    fn from(s: ScheduleIntervalSpec) -> Self {
        Self {
            interval: Some(s.every.try_into().unwrap_or_default()),
            phase: s.offset.and_then(|d| d.try_into().ok()),
        }
    }
}

/// A calendar-based schedule trigger using range strings (e.g., `"2-7"` for hours 2 through 7).
///
/// Empty strings use server defaults (typically `"*"` for most fields, `"0"` for seconds/minutes).
#[derive(Debug, Clone, Default, PartialEq, bon::Builder)]
#[builder(on(String, into))]
#[non_exhaustive]
pub struct ScheduleCalendarSpec {
    /// Second within the minute. Default: `"0"`.
    #[builder(default)]
    pub second: String,
    /// Minute within the hour. Default: `"0"`.
    #[builder(default)]
    pub minute: String,
    /// Hour of the day. Default: `"0"`.
    #[builder(default)]
    pub hour: String,
    /// Day of the month. Default: `"*"`.
    #[builder(default)]
    pub day_of_month: String,
    /// Month of the year. Default: `"*"`.
    #[builder(default)]
    pub month: String,
    /// Day of the week. Default: `"*"`.
    #[builder(default)]
    pub day_of_week: String,
    /// Year. Default: `"*"`.
    #[builder(default)]
    pub year: String,
    /// Free-form comment.
    #[builder(default)]
    pub comment: String,
}

impl From<ScheduleCalendarSpec> for schedule_proto::CalendarSpec {
    fn from(s: ScheduleCalendarSpec) -> Self {
        Self {
            second: s.second,
            minute: s.minute,
            hour: s.hour,
            day_of_month: s.day_of_month,
            month: s.month,
            day_of_week: s.day_of_week,
            year: s.year,
            comment: s.comment,
        }
    }
}

/// Options for listing schedules.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct ListSchedulesOptions {
    /// Maximum number of results per page (server-side hint).
    #[builder(default)]
    pub maximum_page_size: i32,
    /// Query filter string.
    #[builder(default)]
    pub query: String,
    /// Controls for each list page RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for deleting a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct DeleteScheduleOptions {
    /// Controls for the delete RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for pausing a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct PauseScheduleOptions {
    /// Controls for the pause RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for unpausing a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct UnpauseScheduleOptions {
    /// Controls for the unpause RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for triggering a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct TriggerScheduleOptions {
    /// Controls for the trigger RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// Options for backfilling a schedule.
#[derive(Debug, Clone, Default, bon::Builder)]
#[non_exhaustive]
pub struct BackfillScheduleOptions {
    /// Controls for the backfill RPC.
    #[builder(default)]
    pub rpc_options: RpcOptions,
}

/// A stream of schedule summaries from a list operation.
/// Internally paginates through results from the server.
pub struct ListSchedulesStream {
    inner: Pin<Box<dyn futures_util::Stream<Item = Result<ScheduleSummary, ScheduleError>> + Send>>,
}

impl ListSchedulesStream {
    pub(crate) fn new(
        inner: Pin<
            Box<dyn futures_util::Stream<Item = Result<ScheduleSummary, ScheduleError>> + Send>,
        >,
    ) -> Self {
        Self { inner }
    }
}

impl futures_util::Stream for ListSchedulesStream {
    type Item = Result<ScheduleSummary, ScheduleError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// A recent action taken by a schedule.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ScheduleRecentAction {
    /// When this action was scheduled to occur (including jitter).
    pub schedule_time: Option<SystemTime>,
    /// When this action actually occurred.
    pub actual_time: Option<SystemTime>,
    /// Workflow ID of the started workflow.
    pub workflow_id: String,
    /// Run ID of the started workflow.
    pub run_id: String,
}

/// A currently-running workflow started by a schedule.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ScheduleRunningAction {
    /// Workflow ID of the running workflow.
    pub workflow_id: String,
    /// Run ID of the running workflow.
    pub run_id: String,
}

/// The action configured on a described schedule.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ScheduleDescriptionAction {
    /// Start a workflow execution.
    StartWorkflow(ScheduleDescriptionStartWorkflowAction),
}

impl ScheduleDescriptionAction {
    fn from_proto(
        action: &schedule_proto::ScheduleAction,
        data_converter: DataConverter,
    ) -> Option<Self> {
        match action.action.as_ref()? {
            schedule_proto::schedule_action::Action::StartWorkflow(info) => {
                Some(Self::StartWorkflow(
                    ScheduleDescriptionStartWorkflowAction::from_proto(info, data_converter),
                ))
            }
        }
    }
}

/// Start-workflow action details returned by a schedule description.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ScheduleDescriptionStartWorkflowAction {
    workflow_type: String,
    task_queue: String,
    workflow_id: String,
    input: Option<common_proto::Payloads>,
    data_converter: DataConverter,
}

impl ScheduleDescriptionStartWorkflowAction {
    fn from_proto(
        info: &workflow_proto::NewWorkflowExecutionInfo,
        data_converter: DataConverter,
    ) -> Self {
        Self {
            workflow_type: info
                .workflow_type
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default(),
            task_queue: info
                .task_queue
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default(),
            workflow_id: info.workflow_id.clone(),
            input: info.input.clone(),
            data_converter,
        }
    }

    /// The workflow type name.
    pub fn workflow_type(&self) -> &str {
        &self.workflow_type
    }

    /// The task queue to run the workflow on.
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// The workflow ID configured on the schedule action.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// Returns the workflow arguments deserialized as the requested type, if present.
    pub async fn args<T: TemporalDeserializable + 'static>(
        &self,
    ) -> Result<Option<T>, PayloadConversionError> {
        match &self.input {
            Some(input) => self
                .data_converter
                .from_payloads(&SerializationContextData::Workflow, input.payloads.clone())
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Returns the raw workflow argument payloads, if present.
    pub fn raw_args(&self) -> Option<&[common_proto::Payload]> {
        self.input.as_ref().map(|input| input.payloads.as_slice())
    }
}

impl From<&schedule_proto::ScheduleActionResult> for ScheduleRecentAction {
    fn from(a: &schedule_proto::ScheduleActionResult) -> Self {
        let workflow_result = a
            .start_workflow_result
            .as_ref()
            .expect("unsupported schedule action: start_workflow_result should be present");
        ScheduleRecentAction {
            schedule_time: a.schedule_time.as_ref().and_then(proto_ts_to_system_time),
            actual_time: a.actual_time.as_ref().and_then(proto_ts_to_system_time),
            workflow_id: workflow_result.workflow_id.clone(),
            run_id: workflow_result.run_id.clone(),
        }
    }
}

/// Description of a schedule returned by describe().
///
/// Provides ergonomic accessors over the raw `DescribeScheduleResponse` proto.
/// Use [`raw()`](Self::raw) or [`into_raw()`](Self::into_raw) to access the
/// full proto when needed.
#[derive(Debug, Clone)]
pub struct ScheduleDescription {
    raw: DescribeScheduleResponse,
    data_converter: DataConverter,
}

impl ScheduleDescription {
    pub(crate) fn new(
        raw: DescribeScheduleResponse,
        data_converter: DataConverter,
        schedule_id: &str,
    ) -> Result<Self, ScheduleError> {
        let action = raw
            .schedule
            .as_ref()
            .ok_or_else(|| Self::malformed_description_error(schedule_id, "missing schedule"))?
            .action
            .as_ref()
            .ok_or_else(|| {
                Self::malformed_description_error(schedule_id, "missing schedule action")
            })?;
        if action.action.is_none() {
            return Err(Self::malformed_description_error(
                schedule_id,
                "missing schedule action variant",
            ));
        }
        Ok(Self {
            raw,
            data_converter,
        })
    }

    fn malformed_description_error(schedule_id: &str, reason: impl Into<String>) -> ScheduleError {
        ScheduleError::MalformedDescription {
            schedule_id: schedule_id.to_string(),
            reason: reason.into(),
        }
    }

    /// Token used for optimistic concurrency on updates.
    pub fn conflict_token(&self) -> &[u8] {
        &self.raw.conflict_token
    }

    /// The action configured on this schedule.
    pub fn action(&self) -> ScheduleDescriptionAction {
        let action = self
            .raw
            .schedule
            .as_ref()
            .expect("schedule description should contain schedule")
            .action
            .as_ref()
            .expect("schedule description should contain action");
        ScheduleDescriptionAction::from_proto(action, self.data_converter.clone())
            .expect("schedule action should contain an action variant")
    }

    /// Whether the schedule is paused.
    pub fn paused(&self) -> bool {
        self.raw
            .schedule
            .as_ref()
            .and_then(|s| s.state.as_ref())
            .is_some_and(|st| st.paused)
    }

    /// Note on the schedule state (e.g., reason for pause).
    /// Returns `None` if no note is set or the note is empty.
    pub fn note(&self) -> Option<&str> {
        self.raw
            .schedule
            .as_ref()
            .and_then(|s| s.state.as_ref())
            .map(|st| st.notes.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Total number of actions taken by this schedule.
    pub fn action_count(&self) -> i64 {
        self.info().map_or(0, |i| i.action_count)
    }

    /// Number of times a scheduled action was skipped due to missing the catchup window.
    pub fn missed_catchup_window(&self) -> i64 {
        self.info().map_or(0, |i| i.missed_catchup_window)
    }

    /// Number of skipped actions due to overlap.
    pub fn overlap_skipped(&self) -> i64 {
        self.info().map_or(0, |i| i.overlap_skipped)
    }

    /// Most recent action results (up to 10).
    pub fn recent_actions(&self) -> Vec<ScheduleRecentAction> {
        self.info()
            .map(|i| {
                i.recent_actions
                    .iter()
                    .map(ScheduleRecentAction::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Currently-running workflows started by this schedule.
    pub fn running_actions(&self) -> Vec<ScheduleRunningAction> {
        self.info()
            .map(|i| {
                i.running_workflows
                    .iter()
                    .map(|w| ScheduleRunningAction {
                        workflow_id: w.workflow_id.clone(),
                        run_id: w.run_id.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Next scheduled action times.
    pub fn future_action_times(&self) -> Vec<SystemTime> {
        self.info()
            .map(|i| {
                i.future_action_times
                    .iter()
                    .filter_map(proto_ts_to_system_time)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// When the schedule was created.
    pub fn create_time(&self) -> Option<SystemTime> {
        self.info()
            .and_then(|i| i.create_time.as_ref())
            .and_then(proto_ts_to_system_time)
    }

    /// When the schedule was last updated.
    pub fn update_time(&self) -> Option<SystemTime> {
        self.info()
            .and_then(|i| i.update_time.as_ref())
            .and_then(proto_ts_to_system_time)
    }

    /// Memo attached to the schedule, decoded with the client's payload converter.
    pub fn memo(&self) -> crate::Memo {
        crate::Memo::from_raw(
            self.raw.memo.clone(),
            self.data_converter.payload_converter().clone(),
            SerializationContextData::Workflow,
        )
    }

    /// Search attributes on the schedule.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.raw
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
            .unwrap_or_default()
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &DescribeScheduleResponse {
        &self.raw
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> DescribeScheduleResponse {
        self.raw
    }

    fn info(&self) -> Option<&schedule_proto::ScheduleInfo> {
        self.raw.info.as_ref()
    }

    /// Convert this description into a [`ScheduleUpdate`] for use with
    /// [`ScheduleHandle::send_update()`].
    ///
    /// Extracts the schedule definition from the description.
    pub fn into_update(self) -> ScheduleUpdate {
        ScheduleUpdate {
            schedule: self.raw.schedule.unwrap_or_default(),
            pending_action: None,
        }
    }
}

/// Controls what happens when a scheduled workflow would overlap with a running one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ScheduleOverlapPolicy {
    /// Use the server default (currently Skip).
    #[default]
    Unspecified,
    /// Don't start a new workflow if one is already running.
    Skip,
    /// Buffer one workflow start, to run after the current one completes.
    BufferOne,
    /// Buffer all workflow starts, to run sequentially.
    BufferAll,
    /// Cancel the running workflow and start a new one.
    CancelOther,
    /// Terminate the running workflow and start a new one.
    TerminateOther,
    /// Start any number of concurrent workflows.
    AllowAll,
}

impl ScheduleOverlapPolicy {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::Unspecified => 0,
            Self::Skip => 1,
            Self::BufferOne => 2,
            Self::BufferAll => 3,
            Self::CancelOther => 4,
            Self::TerminateOther => 5,
            Self::AllowAll => 6,
        }
    }
}

/// A backfill request for a schedule, specifying a time range of missed runs.
#[derive(Debug, Clone, PartialEq, bon::Builder)]
#[non_exhaustive]
#[builder(start_fn = new)]
pub struct ScheduleBackfill {
    /// Start of the time range to backfill.
    #[builder(start_fn)]
    pub start_time: SystemTime,
    /// End of the time range to backfill.
    #[builder(start_fn)]
    pub end_time: SystemTime,
    /// How overlapping runs are handled during backfill.
    #[builder(default)]
    pub overlap_policy: ScheduleOverlapPolicy,
}

/// An update to apply to a schedule definition.
///
/// Obtain from [`ScheduleDescription::into_update()`], modify the schedule
/// using the setter methods, then pass to [`ScheduleHandle::update()`].
#[derive(Debug, Clone)]
pub struct ScheduleUpdate {
    schedule: schedule_proto::Schedule,
    pending_action: Option<ScheduleAction>,
}

impl ScheduleUpdate {
    /// Replace the schedule spec (when to trigger).
    pub fn set_spec(&mut self, spec: ScheduleSpec) -> &mut Self {
        self.schedule.spec = Some(spec.into_proto());
        self
    }

    /// Replace the schedule action (what to do on trigger).
    pub fn set_action(&mut self, action: ScheduleAction) -> &mut Self {
        self.pending_action = Some(action);
        self
    }

    /// Set whether the schedule is paused.
    pub fn set_paused(&mut self, paused: bool) -> &mut Self {
        self.state_mut().paused = paused;
        self
    }

    /// Set the note on the schedule state.
    pub fn set_note(&mut self, note: impl Into<String>) -> &mut Self {
        self.state_mut().notes = note.into();
        self
    }

    /// Set the overlap policy.
    pub fn set_overlap_policy(&mut self, policy: ScheduleOverlapPolicy) -> &mut Self {
        self.policies_mut().overlap_policy = policy.to_proto();
        self
    }

    /// Set the catchup window. Actions missed by more than this duration are
    /// skipped.
    pub fn set_catchup_window(&mut self, window: Duration) -> &mut Self {
        self.policies_mut().catchup_window = window.try_into().ok();
        self
    }

    /// Set whether to pause the schedule when a workflow run fails or times out.
    pub fn set_pause_on_failure(&mut self, pause_on_failure: bool) -> &mut Self {
        self.policies_mut().pause_on_failure = pause_on_failure;
        self
    }

    /// Set whether to keep the original workflow ID without appending a
    /// timestamp.
    pub fn set_keep_original_workflow_id(&mut self, keep: bool) -> &mut Self {
        self.policies_mut().keep_original_workflow_id = keep;
        self
    }

    /// Limit the schedule to a fixed number of remaining actions, after which
    /// it stops triggering. Passing `None` removes the limit.
    pub fn set_remaining_actions(&mut self, count: Option<i64>) -> &mut Self {
        let state = self.state_mut();
        match count {
            Some(n) => {
                state.limited_actions = true;
                state.remaining_actions = n;
            }
            None => {
                state.limited_actions = false;
                state.remaining_actions = 0;
            }
        }
        self
    }

    /// Access the raw schedule proto.
    pub fn raw(&self) -> &schedule_proto::Schedule {
        &self.schedule
    }

    /// Consume and return the raw schedule proto.
    pub fn into_raw(self) -> schedule_proto::Schedule {
        self.schedule
    }

    fn state_mut(&mut self) -> &mut schedule_proto::ScheduleState {
        self.schedule.state.get_or_insert_with(Default::default)
    }

    fn policies_mut(&mut self) -> &mut schedule_proto::SchedulePolicies {
        self.schedule.policies.get_or_insert_with(Default::default)
    }
}

/// Summary of a schedule returned in list operations.
///
/// Provides ergonomic accessors over the raw `ScheduleListEntry` proto.
/// Use [`raw()`](Self::raw) or [`into_raw()`](Self::into_raw) to access the
/// full proto when needed.
#[derive(Debug, Clone)]
pub struct ScheduleSummary {
    raw: schedule_proto::ScheduleListEntry,
    data_converter: DataConverter,
}

impl ScheduleSummary {
    /// The schedule ID.
    pub fn schedule_id(&self) -> &str {
        &self.raw.schedule_id
    }

    /// The workflow type name for start-workflow actions.
    pub fn workflow_type(&self) -> Option<&str> {
        self.info()
            .and_then(|i| i.workflow_type.as_ref())
            .map(|wt| wt.name.as_str())
    }

    /// Note on the schedule state.
    /// Returns `None` if no note is set or the note is empty.
    pub fn note(&self) -> Option<&str> {
        self.info()
            .map(|i| i.notes.as_str())
            .filter(|s| !s.is_empty())
    }

    /// Whether the schedule is paused.
    pub fn paused(&self) -> bool {
        self.info().is_some_and(|i| i.paused)
    }

    /// Most recent action results (up to 10).
    pub fn recent_actions(&self) -> Vec<ScheduleRecentAction> {
        self.info()
            .map(|i| {
                i.recent_actions
                    .iter()
                    .map(ScheduleRecentAction::from)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Next scheduled action times.
    pub fn future_action_times(&self) -> Vec<SystemTime> {
        self.info()
            .map(|i| {
                i.future_action_times
                    .iter()
                    .filter_map(proto_ts_to_system_time)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Memo attached to the schedule, decoded with the client's payload converter.
    pub fn memo(&self) -> crate::Memo {
        crate::Memo::from_raw(
            self.raw.memo.clone(),
            self.data_converter.payload_converter().clone(),
            SerializationContextData::Workflow,
        )
    }

    /// Search attributes on the schedule.
    pub fn search_attributes(&self) -> SearchAttributes {
        self.raw
            .search_attributes
            .as_ref()
            .map(SearchAttributes::from_proto)
            .unwrap_or_default()
    }

    /// Access the raw proto for additional fields not exposed via accessors.
    pub fn raw(&self) -> &schedule_proto::ScheduleListEntry {
        &self.raw
    }

    /// Consume the wrapper and return the raw proto.
    pub fn into_raw(self) -> schedule_proto::ScheduleListEntry {
        self.raw
    }

    fn info(&self) -> Option<&schedule_proto::ScheduleListInfo> {
        self.raw.info.as_ref()
    }
}

impl From<schedule_proto::ScheduleListEntry> for ScheduleSummary {
    fn from(raw: schedule_proto::ScheduleListEntry) -> Self {
        Self::new(raw, DataConverter::default())
    }
}

impl ScheduleSummary {
    fn new(raw: schedule_proto::ScheduleListEntry, data_converter: DataConverter) -> Self {
        Self {
            raw,
            data_converter,
        }
    }
}

/// Handle to an existing schedule. Obtained from
/// [`Client::create_schedule`](crate::Client::create_schedule) or
/// [`Client::get_schedule_handle`](crate::Client::get_schedule_handle).
#[derive(Clone, derive_more::Debug)]
pub struct ScheduleHandle<CT> {
    #[debug(skip)]
    client: CT,
    namespace: String,
    schedule_id: String,
}

impl<CT> ScheduleHandle<CT>
where
    CT: WorkflowService + NamespacedClient + Clone + Send + Sync,
{
    pub(crate) fn new(client: CT, namespace: String, schedule_id: String) -> Self {
        Self {
            client,
            namespace,
            schedule_id,
        }
    }

    /// The namespace the schedule belongs to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The schedule ID.
    pub fn schedule_id(&self) -> &str {
        &self.schedule_id
    }

    /// Describe this schedule, returning its full definition, info, and conflict token.
    pub async fn describe(
        &self,
        rpc_options: RpcOptions,
    ) -> Result<ScheduleDescription, ScheduleError> {
        let output = interceptors::call_describe_schedule(
            self.client.client_interceptors(),
            DescribeScheduleInput {
                schedule_id: self.schedule_id.clone(),
                rpc_options,
            },
            Next::new({
                let handle = self.clone();
                move |input: DescribeScheduleInput| -> BoxFuture<
                    '_,
                    Result<DescribeScheduleOutput, ScheduleError>,
                > {
                    Box::pin(async move {
                        let response = handle
                            .describe_raw(input.schedule_id, input.rpc_options)
                            .await?;
                        Ok(DescribeScheduleOutput::new(response))
                    })
                }
            }),
        )
        .await?;

        let mut resp = output.response;
        if let Some(memo) = resp.memo.as_mut() {
            decode_payloads(
                memo,
                self.client.data_converter().codec(),
                &SerializationContextData::Workflow,
            )
            .await?;
        }

        ScheduleDescription::new(
            resp,
            self.client.data_converter().clone(),
            &self.schedule_id,
        )
    }

    /// Update the schedule definition.
    ///
    /// Describes the current schedule, applies the closure to modify it, and
    /// sends the update. The conflict token is managed automatically.
    ///
    /// ```no_run
    /// # async fn hidden(
    /// #     handle: &temporalio_client::schedules::ScheduleHandle<temporalio_client::Client>,
    /// # ) -> Result<(), temporalio_client::schedules::ScheduleError> {
    /// handle
    ///     .update(
    ///         |u| {
    ///             u.set_note("updated").set_paused(true);
    ///         },
    ///         Default::default(),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    // TODO: Add a retry loop for conflict token mismatch. The server
    // returns FailedPrecondition with "mismatched conflict token".
    pub async fn update(
        &self,
        updater: impl FnOnce(&mut ScheduleUpdate) + Send + 'static,
        rpc_options: RpcOptions,
    ) -> Result<(), ScheduleError> {
        interceptors::call_update_schedule(
            self.client.client_interceptors(),
            UpdateScheduleInput {
                schedule_id: self.schedule_id.clone(),
                rpc_options,
            },
            Next::new({
                let handle = self.clone();
                move |input: UpdateScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let mut response = handle
                            .describe_raw(input.schedule_id.clone(), input.rpc_options.clone())
                            .await?;
                        decode_payloads(
                            &mut response,
                            handle.client.data_converter().codec(),
                            &SerializationContextData::Workflow,
                        )
                        .await?;
                        let description = ScheduleDescription::new(
                            response,
                            handle.client.data_converter().clone(),
                            &input.schedule_id,
                        )?;
                        let mut update = description.into_update();
                        updater(&mut update);
                        handle
                            .send_update_raw(input.schedule_id, update, input.rpc_options)
                            .await
                    })
                }
            }),
        )
        .await
    }

    /// Send a pre-built [`ScheduleUpdate`] to the server.
    ///
    /// Prefer [`update()`](Self::update) for most use cases. Use this when you
    /// need to inspect the [`ScheduleDescription`] before deciding what to
    /// change.
    pub async fn send_update(
        &self,
        update: ScheduleUpdate,
        rpc_options: RpcOptions,
    ) -> Result<(), ScheduleError> {
        interceptors::call_send_schedule_update(
            self.client.client_interceptors(),
            SendScheduleUpdateInput {
                schedule_id: self.schedule_id.clone(),
                update,
                rpc_options,
            },
            Next::new({
                let handle = self.clone();
                move |input: SendScheduleUpdateInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        handle
                            .send_update_raw(input.schedule_id, input.update, input.rpc_options)
                            .await
                    })
                }
            }),
        )
        .await
    }

    async fn describe_raw(
        &self,
        schedule_id: String,
        rpc_options: RpcOptions,
    ) -> Result<DescribeScheduleResponse, ScheduleError> {
        let mut request = DescribeScheduleRequest {
            namespace: self.namespace.clone(),
            schedule_id,
        }
        .into_request();
        rpc_options.apply_to(&mut request);
        Ok(
            WorkflowService::describe_schedule(&mut self.client.clone(), request)
                .await?
                .into_inner(),
        )
    }

    async fn send_update_raw(
        &self,
        schedule_id: String,
        mut update: ScheduleUpdate,
        rpc_options: RpcOptions,
    ) -> Result<(), ScheduleError> {
        if let Some(action) = update.pending_action.take() {
            update.schedule.action = Some(action.into_proto(self.client.data_converter()).await?);
        }
        let mut request = UpdateScheduleRequest {
            namespace: self.namespace.clone(),
            schedule_id,
            schedule: Some(update.schedule),
            identity: self.client.identity(),
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }
        .into_request();
        rpc_options.apply_to(&mut request);
        WorkflowService::update_schedule(&mut self.client.clone(), request).await?;
        Ok(())
    }

    /// Delete this schedule.
    pub async fn delete(&self, options: DeleteScheduleOptions) -> Result<(), ScheduleError> {
        let rpc_options = options.rpc_options;
        interceptors::call_delete_schedule(
            self.client.client_interceptors(),
            DeleteScheduleInput {
                schedule_id: self.schedule_id.clone(),
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let namespace = self.namespace.clone();
                move |input: DeleteScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let mut request = DeleteScheduleRequest {
                            namespace,
                            schedule_id: input.schedule_id,
                            identity: client.identity(),
                        }
                        .into_request();
                        input.rpc_options.apply_to(&mut request);
                        WorkflowService::delete_schedule(&mut client, request).await?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Pause the schedule with an optional note.
    ///
    /// If `note` is `None`, a default note is used.
    pub async fn pause(
        &self,
        note: Option<impl Into<String>>,
        options: PauseScheduleOptions,
    ) -> Result<(), ScheduleError> {
        let note = note.map_or_else(|| "Paused via Rust SDK".to_string(), |s| s.into());
        let rpc_options = options.rpc_options;
        interceptors::call_pause_schedule(
            self.client.client_interceptors(),
            PauseScheduleInput {
                schedule_id: self.schedule_id.clone(),
                note,
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let namespace = self.namespace.clone();
                move |input: PauseScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let mut request = PatchScheduleRequest {
                            namespace,
                            schedule_id: input.schedule_id,
                            patch: Some(schedule_proto::SchedulePatch {
                                pause: input.note,
                                ..Default::default()
                            }),
                            identity: client.identity(),
                            request_id: Uuid::new_v4().to_string(),
                        }
                        .into_request();
                        input.rpc_options.apply_to(&mut request);
                        WorkflowService::patch_schedule(&mut client, request).await?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Unpause the schedule with an optional note.
    ///
    /// If `note` is `None`, a default note is used.
    pub async fn unpause(
        &self,
        note: Option<impl Into<String>>,
        options: UnpauseScheduleOptions,
    ) -> Result<(), ScheduleError> {
        let note = note.map_or_else(|| "Unpaused via Rust SDK".to_string(), |s| s.into());
        let rpc_options = options.rpc_options;
        interceptors::call_unpause_schedule(
            self.client.client_interceptors(),
            UnpauseScheduleInput {
                schedule_id: self.schedule_id.clone(),
                note,
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let namespace = self.namespace.clone();
                move |input: UnpauseScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let mut request = PatchScheduleRequest {
                            namespace,
                            schedule_id: input.schedule_id,
                            patch: Some(schedule_proto::SchedulePatch {
                                unpause: input.note,
                                ..Default::default()
                            }),
                            identity: client.identity(),
                            request_id: Uuid::new_v4().to_string(),
                        }
                        .into_request();
                        input.rpc_options.apply_to(&mut request);
                        WorkflowService::patch_schedule(&mut client, request).await?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Trigger the schedule to run immediately with the given overlap policy.
    pub async fn trigger(
        &self,
        overlap_policy: ScheduleOverlapPolicy,
        options: TriggerScheduleOptions,
    ) -> Result<(), ScheduleError> {
        let rpc_options = options.rpc_options;
        interceptors::call_trigger_schedule(
            self.client.client_interceptors(),
            TriggerScheduleInput {
                schedule_id: self.schedule_id.clone(),
                overlap_policy,
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let namespace = self.namespace.clone();
                move |input: TriggerScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let mut request = PatchScheduleRequest {
                            namespace,
                            schedule_id: input.schedule_id,
                            patch: Some(schedule_proto::SchedulePatch {
                                trigger_immediately: Some(
                                    schedule_proto::TriggerImmediatelyRequest {
                                        overlap_policy: input.overlap_policy.to_proto(),
                                        scheduled_time: None,
                                    },
                                ),
                                ..Default::default()
                            }),
                            identity: client.identity(),
                            request_id: Uuid::new_v4().to_string(),
                        }
                        .into_request();
                        input.rpc_options.apply_to(&mut request);
                        WorkflowService::patch_schedule(&mut client, request).await?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }

    /// Request backfill of missed runs.
    pub async fn backfill(
        &self,
        backfills: impl IntoIterator<Item = ScheduleBackfill>,
        options: BackfillScheduleOptions,
    ) -> Result<(), ScheduleError> {
        let rpc_options = options.rpc_options;
        interceptors::call_backfill_schedule(
            self.client.client_interceptors(),
            BackfillScheduleInput {
                schedule_id: self.schedule_id.clone(),
                backfills: backfills.into_iter().collect(),
                rpc_options,
            },
            Next::new({
                let mut client = self.client.clone();
                let namespace = self.namespace.clone();
                move |input: BackfillScheduleInput| -> BoxFuture<'_, Result<(), ScheduleError>> {
                    Box::pin(async move {
                        let backfill_requests = input
                            .backfills
                            .into_iter()
                            .map(|backfill| schedule_proto::BackfillRequest {
                                start_time: Some(backfill.start_time.into()),
                                end_time: Some(backfill.end_time.into()),
                                overlap_policy: backfill.overlap_policy.to_proto(),
                            })
                            .collect();
                        let mut request = PatchScheduleRequest {
                            namespace,
                            schedule_id: input.schedule_id,
                            patch: Some(schedule_proto::SchedulePatch {
                                backfill_request: backfill_requests,
                                ..Default::default()
                            }),
                            identity: client.identity(),
                            request_id: Uuid::new_v4().to_string(),
                        }
                        .into_request();
                        input.rpc_options.apply_to(&mut request);
                        WorkflowService::patch_schedule(&mut client, request).await?;
                        Ok(())
                    })
                }
            }),
        )
        .await
    }
}

// Schedule operations on Client.
impl Client {
    /// Create a schedule and return a handle to it.
    pub async fn create_schedule(
        &self,
        schedule_id: impl Into<String>,
        opts: CreateScheduleOptions,
    ) -> Result<ScheduleHandle<Self>, ScheduleError> {
        let schedule_id = schedule_id.into();
        let namespace = self.namespace();
        let output = interceptors::call_create_schedule(
            self.client_interceptors(),
            CreateScheduleInput {
                schedule_id,
                options: opts,
            },
            Next::new({
                let mut client = self.clone();
                move |input: CreateScheduleInput| -> BoxFuture<
                    '_,
                    Result<CreateScheduleOutput, ScheduleError>,
                > {
                    Box::pin(async move {
                        let options = input.options;
                        let initial_patch = options.trigger_immediately.then(|| {
                            schedule_proto::SchedulePatch {
                                trigger_immediately: Some(
                                    schedule_proto::TriggerImmediatelyRequest {
                                        overlap_policy: ScheduleOverlapPolicy::AllowAll.to_proto(),
                                        scheduled_time: None,
                                    },
                                ),
                                ..Default::default()
                            }
                        });
                        let policies = (options.overlap_policy
                            != ScheduleOverlapPolicy::Unspecified)
                            .then(|| schedule_proto::SchedulePolicies {
                                overlap_policy: options.overlap_policy.to_proto(),
                                ..Default::default()
                            });
                        let schedule = schedule_proto::Schedule {
                            spec: Some(options.spec.into_proto()),
                            action: Some(
                                options.action.into_proto(client.data_converter()).await?,
                            ),
                            policies,
                            state: Some(schedule_proto::ScheduleState {
                                paused: options.paused,
                                notes: options.note,
                                ..Default::default()
                            }),
                        };
                        let mut request = CreateScheduleRequest {
                            namespace: client.namespace(),
                            schedule_id: input.schedule_id.clone(),
                            schedule: Some(schedule),
                            initial_patch,
                            identity: client.identity(),
                            request_id: Uuid::new_v4().to_string(),
                            ..Default::default()
                        }
                        .into_request();
                        options.rpc_options.apply_to(&mut request);
                        WorkflowService::create_schedule(&mut client, request).await?;
                        Ok(CreateScheduleOutput::new(input.schedule_id))
                    })
                }
            }),
        )
        .await?;
        Ok(ScheduleHandle::new(
            self.clone(),
            namespace,
            output.schedule_id,
        ))
    }

    /// Get a handle to an existing schedule by ID.
    pub fn get_schedule_handle(&self, schedule_id: impl Into<String>) -> ScheduleHandle<Self> {
        ScheduleHandle::new(self.clone(), self.namespace(), schedule_id.into())
    }

    /// List schedules matching the query, returning a stream that lazily
    /// paginates through results.
    pub fn list_schedules(&self, opts: ListSchedulesOptions) -> ListSchedulesStream {
        list_schedules_stream(self.clone(), opts)
    }
}

fn list_schedules_stream<CT>(client: CT, opts: ListSchedulesOptions) -> ListSchedulesStream
where
    CT: WorkflowService + NamespacedClient + Clone + Send + Sync + 'static,
{
    let namespace = client.namespace();
    let query = opts.query;
    let page_size = opts.maximum_page_size;
    let rpc_options = opts.rpc_options;

    let stream = stream::unfold(
        (Vec::new(), VecDeque::new(), false),
        move |(next_page_token, mut buffer, exhausted)| {
            let client = client.clone();
            let namespace = namespace.clone();
            let query = query.clone();
            let rpc_options = rpc_options.clone();

            async move {
                if let Some(item) = buffer.pop_front() {
                    return Some((Ok(item), (next_page_token, buffer, exhausted)));
                } else if exhausted {
                    return None;
                }

                let response = interceptors::call_list_schedules_page(
                    client.client_interceptors(),
                    ListSchedulesPageInput {
                        maximum_page_size: page_size,
                        query,
                        next_page_token: next_page_token.clone(),
                        rpc_options,
                    },
                    Next::new({
                        let mut rpc_client = client.clone();
                        move |input: ListSchedulesPageInput| -> BoxFuture<
                                '_,
                                Result<ListSchedulesPageOutput, ScheduleError>,
                            > {
                                Box::pin(async move {
                                    let mut request = ListSchedulesRequest {
                                        namespace,
                                        maximum_page_size: input.maximum_page_size,
                                        next_page_token: input.next_page_token,
                                        query: input.query,
                                    }
                                    .into_request();
                                    input.rpc_options.apply_to(&mut request);
                                    let response =
                                        WorkflowService::list_schedules(&mut rpc_client, request)
                                            .await?
                                            .into_inner();
                                    Ok(ListSchedulesPageOutput::new(
                                        response.schedules,
                                        response.next_page_token,
                                    ))
                                })
                            }
                    }),
                )
                .await;

                match response {
                    Ok(mut output) => {
                        let new_exhausted = output.next_page_token.is_empty();
                        let new_token = output.next_page_token;

                        let data_converter = client.data_converter().clone();
                        for schedule in &mut output.schedules {
                            if let Some(memo) = schedule.memo.as_mut()
                                && let Err(err) = decode_payloads(
                                    memo,
                                    data_converter.codec(),
                                    &SerializationContextData::Workflow,
                                )
                                .await
                            {
                                return Some((
                                    Err(ScheduleError::from(err)),
                                    (new_token, buffer, true),
                                ));
                            }
                        }
                        buffer = output
                            .schedules
                            .into_iter()
                            .map(|raw| ScheduleSummary::new(raw, data_converter.clone()))
                            .collect();

                        buffer
                            .pop_front()
                            .map(|item| (Ok(item), (new_token, buffer, new_exhausted)))
                    }
                    Err(e) => Some((Err(e), (next_page_token, buffer, true))),
                }
            }
        },
    );

    ListSchedulesStream::new(Box::pin(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ClientInterceptor, DescribeScheduleInput, DescribeScheduleOutput, NamespacedClient, Next,
        SendScheduleUpdateInput, UpdateScheduleInput,
        grpc::WorkflowService,
        test_helpers::{FailingCodec, XorCodec},
    };
    use futures_util::{FutureExt, StreamExt};
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::SystemTime,
    };
    use temporalio_common::{
        UntypedWorkflow,
        data_converters::{
            DataConverter, DefaultFailureConverter, MultiArgs2, PayloadConverter, RawValue,
        },
        protos::temporal::api::{
            common::v1::{
                Memo, Payload, Payloads, SearchAttributes,
                WorkflowExecution as ProtoWorkflowExecution, WorkflowType,
            },
            schedule::v1::{
                Schedule, ScheduleActionResult, ScheduleInfo, ScheduleListEntry, ScheduleListInfo,
                ScheduleSpec, ScheduleState,
            },
            taskqueue::v1::TaskQueue,
            workflow::v1::NewWorkflowExecutionInfo,
            workflowservice::v1::{
                DeleteScheduleResponse, DescribeScheduleResponse, ListSchedulesResponse,
                PatchScheduleResponse, UpdateScheduleResponse,
            },
        },
    };
    use tonic::{Request, Response};

    fn data_converter_with_codec() -> DataConverter {
        DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            XorCodec,
        )
    }

    #[derive(Default)]
    struct CapturedRequests {
        describe: AtomicUsize,
        update: AtomicUsize,
        delete: AtomicUsize,
        patch: AtomicUsize,
        list: AtomicUsize,
    }

    #[derive(Clone)]
    struct MockScheduleClient {
        captured: Arc<CapturedRequests>,
        describe_response: DescribeScheduleResponse,
        list_response: ListSchedulesResponse,
        data_converter: DataConverter,
        should_error: bool,
        interceptors: Vec<Arc<dyn ClientInterceptor>>,
    }

    impl Default for MockScheduleClient {
        fn default() -> Self {
            Self {
                captured: Arc::new(CapturedRequests::default()),
                describe_response: describe_response_with_start_workflow(None),
                list_response: ListSchedulesResponse::default(),
                data_converter: DataConverter::default(),
                should_error: false,
                interceptors: Vec::new(),
            }
        }
    }

    impl NamespacedClient for MockScheduleClient {
        fn namespace(&self) -> String {
            "test-namespace".to_string()
        }
        fn identity(&self) -> String {
            "test-identity".to_string()
        }
        fn data_converter(&self) -> &DataConverter {
            &self.data_converter
        }
        fn client_interceptors(&self) -> &[Arc<dyn ClientInterceptor>] {
            &self.interceptors
        }
    }

    #[derive(Default)]
    struct ScheduleHookCounts {
        describe: AtomicUsize,
        update: AtomicUsize,
        send_update: AtomicUsize,
    }

    struct RecordingScheduleInterceptor {
        counts: Arc<ScheduleHookCounts>,
    }

    impl ClientInterceptor for RecordingScheduleInterceptor {
        fn describe_schedule<'a>(
            &'a self,
            input: DescribeScheduleInput,
            next: Next<
                'a,
                DescribeScheduleInput,
                BoxFuture<'a, Result<DescribeScheduleOutput, ScheduleError>>,
            >,
        ) -> BoxFuture<'a, Result<DescribeScheduleOutput, ScheduleError>> {
            self.counts.describe.fetch_add(1, Ordering::SeqCst);
            next.run(input)
        }

        fn update_schedule<'a>(
            &'a self,
            input: UpdateScheduleInput,
            next: Next<'a, UpdateScheduleInput, BoxFuture<'a, Result<(), ScheduleError>>>,
        ) -> BoxFuture<'a, Result<(), ScheduleError>> {
            self.counts.update.fetch_add(1, Ordering::SeqCst);
            next.run(input)
        }

        fn send_schedule_update<'a>(
            &'a self,
            input: SendScheduleUpdateInput,
            next: Next<'a, SendScheduleUpdateInput, BoxFuture<'a, Result<(), ScheduleError>>>,
        ) -> BoxFuture<'a, Result<(), ScheduleError>> {
            self.counts.send_update.fetch_add(1, Ordering::SeqCst);
            next.run(input)
        }
    }

    impl WorkflowService for MockScheduleClient {
        fn describe_schedule(
            &mut self,
            _request: Request<DescribeScheduleRequest>,
        ) -> futures_util::future::BoxFuture<
            '_,
            Result<Response<DescribeScheduleResponse>, tonic::Status>,
        > {
            self.captured.describe.fetch_add(1, Ordering::SeqCst);
            let resp = self.describe_response.clone();
            let should_error = self.should_error;
            async move {
                if should_error {
                    Err(tonic::Status::not_found("schedule not found"))
                } else {
                    Ok(Response::new(resp))
                }
            }
            .boxed()
        }

        fn update_schedule(
            &mut self,
            _request: Request<UpdateScheduleRequest>,
        ) -> futures_util::future::BoxFuture<
            '_,
            Result<Response<UpdateScheduleResponse>, tonic::Status>,
        > {
            self.captured.update.fetch_add(1, Ordering::SeqCst);
            let should_error = self.should_error;
            async move {
                if should_error {
                    Err(tonic::Status::internal("update failed"))
                } else {
                    Ok(Response::new(UpdateScheduleResponse::default()))
                }
            }
            .boxed()
        }

        fn delete_schedule(
            &mut self,
            _request: Request<DeleteScheduleRequest>,
        ) -> futures_util::future::BoxFuture<
            '_,
            Result<Response<DeleteScheduleResponse>, tonic::Status>,
        > {
            self.captured.delete.fetch_add(1, Ordering::SeqCst);
            let should_error = self.should_error;
            async move {
                if should_error {
                    Err(tonic::Status::internal("delete failed"))
                } else {
                    Ok(Response::new(DeleteScheduleResponse::default()))
                }
            }
            .boxed()
        }

        fn patch_schedule(
            &mut self,
            _request: Request<PatchScheduleRequest>,
        ) -> futures_util::future::BoxFuture<
            '_,
            Result<Response<PatchScheduleResponse>, tonic::Status>,
        > {
            self.captured.patch.fetch_add(1, Ordering::SeqCst);
            let should_error = self.should_error;
            async move {
                if should_error {
                    Err(tonic::Status::internal("patch failed"))
                } else {
                    Ok(Response::new(PatchScheduleResponse::default()))
                }
            }
            .boxed()
        }

        fn list_schedules(
            &mut self,
            _request: Request<ListSchedulesRequest>,
        ) -> futures_util::future::BoxFuture<
            '_,
            Result<Response<ListSchedulesResponse>, tonic::Status>,
        > {
            self.captured.list.fetch_add(1, Ordering::SeqCst);
            let response = self.list_response.clone();
            async move { Ok(Response::new(response)) }.boxed()
        }
    }

    fn make_schedule_handle(client: MockScheduleClient) -> ScheduleHandle<MockScheduleClient> {
        ScheduleHandle::new(
            client,
            "test-namespace".to_string(),
            "test-schedule-id".to_string(),
        )
    }

    fn describe_response_with_start_workflow(input: Option<Payloads>) -> DescribeScheduleResponse {
        DescribeScheduleResponse {
            schedule: Some(Schedule {
                action: Some(schedule_proto::ScheduleAction {
                    action: Some(schedule_proto::schedule_action::Action::StartWorkflow(
                        NewWorkflowExecutionInfo {
                            workflow_id: "wf-id".to_string(),
                            workflow_type: Some(WorkflowType {
                                name: "MyWorkflow".to_string(),
                            }),
                            task_queue: Some(TaskQueue {
                                name: "task-queue".to_string(),
                                ..Default::default()
                            }),
                            input,
                            ..Default::default()
                        },
                    )),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn schedule_description_from_response(raw: DescribeScheduleResponse) -> ScheduleDescription {
        ScheduleDescription::new(raw, DataConverter::default(), "test-schedule-id").unwrap()
    }

    fn describe_response_without_schedule() -> DescribeScheduleResponse {
        DescribeScheduleResponse::default()
    }

    fn describe_response_without_action() -> DescribeScheduleResponse {
        DescribeScheduleResponse {
            schedule: Some(Schedule::default()),
            ..Default::default()
        }
    }

    fn describe_response_without_action_variant() -> DescribeScheduleResponse {
        DescribeScheduleResponse {
            schedule: Some(Schedule {
                action: Some(schedule_proto::ScheduleAction::default()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn schedule_handle_exposes_namespace_and_id() {
        let handle = make_schedule_handle(MockScheduleClient::default());
        assert_eq!(handle.namespace(), "test-namespace");
        assert_eq!(handle.schedule_id(), "test-schedule-id");
    }

    #[tokio::test]
    async fn schedule_describe_returns_response_fields() {
        let conflict_token = b"token-123".to_vec();
        let mut describe_response = describe_response_with_start_workflow(None);
        describe_response.info = Some(ScheduleInfo::default());
        describe_response.memo = Some(Memo {
            fields: Default::default(),
        });
        describe_response.search_attributes = Some(SearchAttributes {
            indexed_fields: Default::default(),
        });
        describe_response.conflict_token = conflict_token.clone();

        let client = MockScheduleClient {
            describe_response,
            ..Default::default()
        };

        let handle = make_schedule_handle(client.clone());
        let desc = handle.describe(RpcOptions::default()).await.unwrap();

        assert_eq!(client.captured.describe.load(Ordering::SeqCst), 1);
        assert!(desc.raw().schedule.is_some());
        assert!(desc.raw().info.is_some());
        assert!(desc.raw().memo.is_some());
        assert!(desc.raw().search_attributes.is_some());
        assert!(desc.search_attributes().is_empty());
        assert_eq!(desc.conflict_token(), conflict_token);
    }

    #[tokio::test]
    async fn schedule_description_exposes_typed_memo() {
        let data_converter = data_converter_with_codec();
        let memo_payload = data_converter
            .to_payload(
                &SerializationContextData::Workflow,
                &"memo-value".to_owned(),
            )
            .await
            .unwrap();
        let mut describe_response = describe_response_with_start_workflow(None);
        describe_response.memo = Some(Memo {
            fields: HashMap::from([("memo-key".to_owned(), memo_payload)]),
        });
        let client = MockScheduleClient {
            describe_response,
            data_converter,
            ..Default::default()
        };

        let description = make_schedule_handle(client)
            .describe(RpcOptions::default())
            .await
            .unwrap();

        assert_eq!(
            description.memo().get::<String>("memo-key").unwrap(),
            Some("memo-value".to_owned())
        );
    }

    #[tokio::test]
    async fn schedule_summary_exposes_typed_memo() {
        let data_converter = DataConverter::default();
        let memo_payload = data_converter
            .to_payload(
                &SerializationContextData::Workflow,
                &"memo-value".to_owned(),
            )
            .await
            .unwrap();
        let summary = ScheduleSummary::new(
            ScheduleListEntry {
                schedule_id: "schedule-id".to_owned(),
                memo: Some(Memo {
                    fields: HashMap::from([("memo-key".to_owned(), memo_payload)]),
                }),
                ..Default::default()
            },
            data_converter,
        );

        assert_eq!(
            summary.memo().get::<String>("memo-key").unwrap(),
            Some("memo-value".to_owned())
        );
    }

    #[tokio::test]
    async fn list_schedules_yields_codec_error_then_ends() {
        let client = MockScheduleClient {
            list_response: ListSchedulesResponse {
                schedules: vec![ScheduleListEntry {
                    memo: Some(Memo {
                        fields: HashMap::from([("memo-key".to_owned(), Payload::default())]),
                    }),
                    ..Default::default()
                }],
                next_page_token: b"next-page".to_vec(),
            },
            data_converter: DataConverter::new(
                PayloadConverter::default(),
                DefaultFailureConverter,
                FailingCodec,
            ),
            ..Default::default()
        };
        let mut stream = list_schedules_stream(client.clone(), ListSchedulesOptions::default());

        let err = stream.next().await.unwrap().unwrap_err();

        assert!(matches!(err, ScheduleError::PayloadConversion(_)));
        assert!(stream.next().await.is_none());
        assert_eq!(client.captured.list.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_update_describes_then_sends() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .update(
                |u| {
                    u.set_note("hi");
                },
                RpcOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(client.captured.describe.load(Ordering::SeqCst), 1);
        assert_eq!(client.captured.update.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_update_does_not_nest_describe_or_send_update_hooks() {
        let counts = Arc::new(ScheduleHookCounts::default());
        let client = MockScheduleClient {
            interceptors: vec![Arc::new(RecordingScheduleInterceptor {
                counts: counts.clone(),
            })],
            ..Default::default()
        };
        let handle = make_schedule_handle(client.clone());

        handle
            .update(
                |update| {
                    update.set_note("updated");
                },
                RpcOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(counts.update.load(Ordering::SeqCst), 1);
        assert_eq!(counts.describe.load(Ordering::SeqCst), 0);
        assert_eq!(counts.send_update.load(Ordering::SeqCst), 0);
        assert_eq!(client.captured.describe.load(Ordering::SeqCst), 1);
        assert_eq!(client.captured.update.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_multiple_updates_each_call_service() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle.update(|_| {}, RpcOptions::default()).await.unwrap();
        handle.update(|_| {}, RpcOptions::default()).await.unwrap();

        assert_eq!(client.captured.update.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn schedule_delete_calls_service() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .delete(DeleteScheduleOptions::default())
            .await
            .unwrap();

        assert_eq!(client.captured.delete.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_pause_calls_patch() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .pause(Some("taking a break"), PauseScheduleOptions::default())
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_pause_with_none_uses_default() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .pause(None::<&str>, PauseScheduleOptions::default())
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_unpause_calls_patch() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .unpause(Some("resuming work"), UnpauseScheduleOptions::default())
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_trigger_calls_patch() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .trigger(
                ScheduleOverlapPolicy::Unspecified,
                TriggerScheduleOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_backfill_calls_patch() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        let now = SystemTime::now();
        handle
            .backfill(
                vec![
                    ScheduleBackfill::new(now, now)
                        .overlap_policy(ScheduleOverlapPolicy::Skip)
                        .build(),
                    ScheduleBackfill::new(now, now)
                        .overlap_policy(ScheduleOverlapPolicy::BufferOne)
                        .build(),
                ],
                BackfillScheduleOptions::default(),
            )
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn schedule_describe_propagates_rpc_errors() {
        let client = MockScheduleClient {
            should_error: true,
            ..Default::default()
        };
        let handle = make_schedule_handle(client);

        let err = handle.describe(RpcOptions::default()).await.unwrap_err();
        assert!(
            matches!(err, ScheduleError::Rpc(_)),
            "expected Rpc variant, got: {err:?}"
        );
        assert!(err.to_string().contains("schedule not found"));
    }

    #[tokio::test]
    async fn schedule_update_propagates_rpc_errors() {
        let client = MockScheduleClient {
            should_error: true,
            ..Default::default()
        };
        let handle = make_schedule_handle(client);

        let err = handle
            .update(|_| {}, RpcOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, ScheduleError::Rpc(_)));
    }

    #[tokio::test]
    async fn schedule_delete_propagates_rpc_errors() {
        let client = MockScheduleClient {
            should_error: true,
            ..Default::default()
        };
        let handle = make_schedule_handle(client);

        let err = handle
            .delete(DeleteScheduleOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, ScheduleError::Rpc(_)));
    }

    #[tokio::test]
    async fn schedule_patch_operations_propagate_rpc_errors() {
        let client = MockScheduleClient {
            should_error: true,
            ..Default::default()
        };
        let handle = make_schedule_handle(client);

        assert!(
            handle
                .pause(Some(""), PauseScheduleOptions::default())
                .await
                .is_err()
        );
        assert!(
            handle
                .unpause(Some(""), UnpauseScheduleOptions::default())
                .await
                .is_err()
        );
        assert!(
            handle
                .trigger(Default::default(), TriggerScheduleOptions::default())
                .await
                .is_err()
        );
        assert!(
            handle
                .backfill(vec![], BackfillScheduleOptions::default())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn schedule_all_patch_operations_call_service() {
        let client = MockScheduleClient::default();
        let handle = make_schedule_handle(client.clone());

        handle
            .pause(Some("p"), PauseScheduleOptions::default())
            .await
            .unwrap();
        handle
            .unpause(Some("u"), UnpauseScheduleOptions::default())
            .await
            .unwrap();
        handle
            .trigger(Default::default(), TriggerScheduleOptions::default())
            .await
            .unwrap();
        handle
            .backfill(vec![], BackfillScheduleOptions::default())
            .await
            .unwrap();

        assert_eq!(client.captured.patch.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn schedule_describe_accessors_with_populated_fields() {
        let mut describe_response = describe_response_with_start_workflow(None);
        let schedule = describe_response.schedule.as_mut().unwrap();
        schedule.spec = Some(ScheduleSpec {
            timezone_name: "US/Eastern".to_string(),
            ..Default::default()
        });
        schedule.state = Some(ScheduleState {
            paused: true,
            notes: "maintenance window".to_string(),
            ..Default::default()
        });
        describe_response.info = Some(ScheduleInfo {
            action_count: 42,
            missed_catchup_window: 3,
            overlap_skipped: 5,
            recent_actions: vec![ScheduleActionResult {
                start_workflow_result: Some(ProtoWorkflowExecution {
                    workflow_id: "ra-wf".to_string(),
                    run_id: "ra-run".to_string(),
                }),
                ..Default::default()
            }],
            running_workflows: vec![ProtoWorkflowExecution {
                workflow_id: "wf-1".to_string(),
                run_id: "run-1".to_string(),
            }],
            create_time: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            update_time: Some(prost_types::Timestamp {
                seconds: 1_700_001_000,
                nanos: 0,
            }),
            future_action_times: vec![prost_types::Timestamp {
                seconds: 1_700_002_000,
                nanos: 0,
            }],
            ..Default::default()
        });
        describe_response.conflict_token = b"tok".to_vec();

        let client = MockScheduleClient {
            describe_response,
            ..Default::default()
        };

        let handle = make_schedule_handle(client);
        let desc = handle.describe(RpcOptions::default()).await.unwrap();

        assert!(desc.paused());
        assert_eq!(desc.note(), Some("maintenance window"));
        assert_eq!(desc.action_count(), 42);
        assert_eq!(desc.missed_catchup_window(), 3);
        assert_eq!(desc.overlap_skipped(), 5);

        assert_eq!(desc.recent_actions().len(), 1);
        assert_eq!(
            desc.running_actions(),
            vec![ScheduleRunningAction {
                workflow_id: "wf-1".to_string(),
                run_id: "run-1".to_string(),
            }]
        );

        assert_eq!(desc.future_action_times().len(), 1);
        assert!(desc.create_time().is_some());
        assert!(desc.update_time().is_some());
    }

    #[tokio::test]
    async fn schedule_describe_defaults_when_nested_fields_are_none() {
        let client = MockScheduleClient::default();

        let handle = make_schedule_handle(client);
        let desc = handle.describe(RpcOptions::default()).await.unwrap();

        assert!(!desc.paused());
        assert_eq!(desc.note(), None);
        assert_eq!(desc.action_count(), 0);
        assert_eq!(desc.missed_catchup_window(), 0);
        assert_eq!(desc.overlap_skipped(), 0);
        assert!(desc.recent_actions().is_empty());
        assert!(desc.running_actions().is_empty());
        assert!(desc.future_action_times().is_empty());
        assert!(desc.create_time().is_none());
        assert!(desc.update_time().is_none());
        assert!(desc.conflict_token().is_empty());
    }

    #[tokio::test]
    async fn schedule_note_returns_none_for_empty_string() {
        let mut describe_response = describe_response_with_start_workflow(None);
        describe_response.schedule.as_mut().unwrap().state = Some(ScheduleState {
            notes: String::new(),
            ..Default::default()
        });

        let client = MockScheduleClient {
            describe_response,
            ..Default::default()
        };

        let handle = make_schedule_handle(client);
        let desc = handle.describe(RpcOptions::default()).await.unwrap();
        assert_eq!(desc.note(), None);
    }

    #[test]
    fn schedule_summary_note_returns_none_for_empty_string() {
        let entry = ScheduleListEntry {
            schedule_id: "s".to_string(),
            info: Some(ScheduleListInfo {
                notes: String::new(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let summary = ScheduleSummary::from(entry);
        assert_eq!(summary.note(), None);
    }

    #[test]
    fn schedule_summary_accessors() {
        let entry = ScheduleListEntry {
            schedule_id: "sched-1".to_string(),
            memo: Some(Memo {
                fields: Default::default(),
            }),
            search_attributes: Some(SearchAttributes {
                indexed_fields: Default::default(),
            }),
            info: Some(ScheduleListInfo {
                spec: Some(ScheduleSpec::default()),
                workflow_type: Some(WorkflowType {
                    name: "MyWorkflow".to_string(),
                }),
                notes: "some note".to_string(),
                paused: true,
                recent_actions: vec![ScheduleActionResult {
                    start_workflow_result: Some(ProtoWorkflowExecution {
                        workflow_id: "ra-wf".to_string(),
                        run_id: "ra-run".to_string(),
                    }),
                    ..Default::default()
                }],
                future_action_times: vec![prost_types::Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }],
                state_size_bytes: 0,
            }),
        };

        let summary = ScheduleSummary::from(entry);
        assert_eq!(summary.schedule_id(), "sched-1");
        assert!(summary.raw().memo.is_some());
        assert!(summary.raw().search_attributes.is_some());
        assert!(summary.search_attributes().is_empty());
        assert_eq!(summary.workflow_type(), Some("MyWorkflow"));
        assert_eq!(summary.note(), Some("some note"));
        assert!(summary.paused());
        assert_eq!(summary.recent_actions().len(), 1);
        assert_eq!(summary.future_action_times().len(), 1);
    }

    #[test]
    fn schedule_summary_defaults_when_info_is_none() {
        let entry = ScheduleListEntry {
            schedule_id: "sched-2".to_string(),
            ..Default::default()
        };

        let summary = ScheduleSummary::from(entry);
        assert_eq!(summary.schedule_id(), "sched-2");
        assert!(summary.raw().memo.is_none());
        assert!(summary.raw().search_attributes.is_none());
        assert!(summary.search_attributes().is_empty());
        assert_eq!(summary.workflow_type(), None);
        assert_eq!(summary.note(), None);
        assert!(!summary.paused());
        assert!(summary.recent_actions().is_empty());
        assert!(summary.future_action_times().is_empty());
    }

    #[test]
    fn schedule_description_raw_round_trip() {
        let mut resp = describe_response_with_start_workflow(None);
        resp.conflict_token = b"ct".to_vec();

        let desc = schedule_description_from_response(resp.clone());
        assert_eq!(desc.raw().conflict_token, b"ct");
        let recovered = desc.into_raw();
        assert_eq!(recovered.conflict_token, resp.conflict_token);
        assert!(recovered.schedule.is_some());
    }

    #[tokio::test]
    async fn schedule_description_action_decodes_start_workflow_args() {
        let data_converter = DataConverter::default();
        let expected = MultiArgs2("hello".to_string(), 42i32);
        let payloads = data_converter
            .to_payloads(&SerializationContextData::Workflow, &expected)
            .await
            .unwrap();
        let desc = ScheduleDescription::new(
            describe_response_with_start_workflow(Some(Payloads { payloads })),
            data_converter,
            "test-schedule-id",
        )
        .unwrap();

        let ScheduleDescriptionAction::StartWorkflow(action) = desc.action();

        assert_eq!(action.workflow_type(), "MyWorkflow");
        assert_eq!(action.task_queue(), "task-queue");
        assert_eq!(action.workflow_id(), "wf-id");
        let decoded: MultiArgs2<String, i32> = action.args().await.unwrap().unwrap();
        assert_eq!(decoded, expected);
    }

    #[tokio::test]
    async fn schedule_description_start_workflow_args_returns_none_without_input() {
        let desc = schedule_description_from_response(describe_response_with_start_workflow(None));
        let ScheduleDescriptionAction::StartWorkflow(action) = desc.action();

        let decoded: Option<String> = action.args().await.unwrap();
        assert_eq!(decoded, None);
    }

    #[tokio::test]
    async fn schedule_description_start_workflow_args_propagates_decode_errors() {
        let data_converter = DataConverter::default();
        let expected: String = "not-an-int".to_string();
        let payloads = data_converter
            .to_payloads(&SerializationContextData::Workflow, &expected)
            .await
            .unwrap();
        let desc = schedule_description_from_response(describe_response_with_start_workflow(Some(
            Payloads { payloads },
        )));
        let ScheduleDescriptionAction::StartWorkflow(action) = desc.action();

        let err = action.args::<i32>().await.unwrap_err();
        assert!(matches!(err, PayloadConversionError::EncodingError(_)));
    }

    #[rstest::rstest]
    #[case::schedule(describe_response_without_schedule(), "missing schedule")]
    #[case::action(describe_response_without_action(), "missing schedule action")]
    #[case::action_variant(
        describe_response_without_action_variant(),
        "missing schedule action variant"
    )]
    #[tokio::test]
    async fn schedule_describe_errors_when_required_field_is_missing(
        #[case] describe_response: DescribeScheduleResponse,
        #[case] reason: &str,
    ) {
        let client = MockScheduleClient {
            describe_response,
            ..Default::default()
        };
        let handle = make_schedule_handle(client);

        let err = handle.describe(RpcOptions::default()).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("Malformed schedule description for schedule ID 'test-schedule-id': {reason}")
        );
    }

    #[test]
    fn schedule_summary_raw_round_trip() {
        let entry = ScheduleListEntry {
            schedule_id: "rt-1".to_string(),
            ..Default::default()
        };
        let summary = ScheduleSummary::from(entry.clone());
        assert_eq!(summary.raw().schedule_id, "rt-1");
        let recovered = summary.into_raw();
        assert_eq!(recovered.schedule_id, entry.schedule_id);
    }

    #[test]
    fn schedule_into_update_preserves_schedule() {
        let mut resp = describe_response_with_start_workflow(None);
        resp.schedule.as_mut().unwrap().state = Some(ScheduleState {
            notes: "my notes".to_string(),
            ..Default::default()
        });
        let desc = schedule_description_from_response(resp);
        let update = desc.into_update();

        assert_eq!(update.raw().state.as_ref().unwrap().notes, "my notes");
    }

    #[test]
    fn schedule_update_setters_are_chainable() {
        let desc = schedule_description_from_response(describe_response_with_start_workflow(None));
        let mut update = desc.into_update();
        update.set_note("chained").set_paused(true);
        assert_eq!(update.raw().state.as_ref().unwrap().notes, "chained");
        assert!(update.raw().state.as_ref().unwrap().paused);
    }

    #[test]
    fn schedule_recent_action_from_proto_with_timestamps() {
        let ts = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let proto = ScheduleActionResult {
            schedule_time: Some(ts),
            actual_time: Some(ts),
            start_workflow_result: Some(ProtoWorkflowExecution {
                workflow_id: "wf-abc".to_string(),
                run_id: "run-xyz".to_string(),
            }),
            ..Default::default()
        };

        let action = ScheduleRecentAction::from(&proto);

        assert!(action.schedule_time.is_some());
        assert!(action.actual_time.is_some());
        assert_eq!(action.workflow_id, "wf-abc");
        assert_eq!(action.run_id, "run-xyz");
    }

    #[test]
    #[should_panic(expected = "unsupported schedule action")]
    fn schedule_recent_action_panics_without_workflow_result() {
        let _ = ScheduleRecentAction::from(&ScheduleActionResult::default());
    }

    #[test]
    fn schedule_overlap_policy_default_is_unspecified() {
        assert_eq!(
            ScheduleOverlapPolicy::default(),
            ScheduleOverlapPolicy::Unspecified
        );
    }

    #[tokio::test]
    async fn schedule_action_start_workflow_with_input_into_proto() {
        let payload = Payload {
            metadata: [("encoding".to_string(), b"json/plain".to_vec())]
                .into_iter()
                .collect(),
            data: b"42".to_vec(),
            ..Default::default()
        };
        let action = ScheduleAction::start_workflow(
            UntypedWorkflow::new("MyWorkflow"),
            RawValue::new(vec![payload.clone()]),
            "my-queue",
            "my-wf-id",
        );
        let proto = action.into_proto(&DataConverter::default()).await.unwrap();
        #[allow(irrefutable_let_patterns)]
        let schedule_proto::schedule_action::Action::StartWorkflow(wf_info) = proto.action.unwrap()
        else {
            panic!("expected StartWorkflow action")
        };
        assert_eq!(wf_info.input.unwrap().payloads, vec![payload]);
    }
}
