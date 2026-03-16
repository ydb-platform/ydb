use crate::{
    HistoryInfo,
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME},
    coresdk::{
        AsJsonPayloadExt, IntoPayloadsExt,
        common::{
            NamespacedWorkflowExecution, build_has_change_marker_details,
            build_local_activity_marker_details,
        },
        external_data::LocalActivityMarkerData,
        workflow_commands::ScheduleActivity,
    },
    temporal::api::{
        common::v1::{
            ActivityType, Payload, Payloads, SearchAttributes, WorkflowExecution, WorkflowType,
        },
        enums::v1::{EventType, TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::{CanceledFailureInfo, Failure, failure},
        history::v1::{history_event::Attributes, *},
        taskqueue::v1::TaskQueue,
        update,
        update::v1::outcome,
    },
};
use anyhow::bail;
use prost_wkt_types::Timestamp;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use uuid::Uuid;

pub static DEFAULT_WORKFLOW_TYPE: &str = "default_wf_type";
pub static DEFAULT_ACTIVITY_TYPE: &str = "default_act_type";

type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

#[derive(Default, Clone, Debug)]
pub struct TestHistoryBuilder {
    events: Vec<HistoryEvent>,
    /// Is incremented every time a new event is added, and that *new* value is used as that event's
    /// id
    current_event_id: i64,
    workflow_task_scheduled_event_id: i64,
    final_workflow_task_started_event_id: i64,
    previous_task_completed_id: i64,
    original_run_id: String,
}

impl TestHistoryBuilder {
    pub fn from_history(events: Vec<HistoryEvent>) -> Self {
        let find_matching_id = |etype: EventType| {
            events
                .iter()
                .rev()
                .find(|e| e.event_type() == etype)
                .map(|e| e.event_id)
                .unwrap_or_default()
        };
        Self {
            current_event_id: events.last().map(|e| e.event_id).unwrap_or_default(),
            workflow_task_scheduled_event_id: find_matching_id(EventType::WorkflowTaskScheduled),
            final_workflow_task_started_event_id: find_matching_id(EventType::WorkflowTaskStarted),
            previous_task_completed_id: find_matching_id(EventType::WorkflowTaskCompleted),
            original_run_id: extract_original_run_id_from_events(&events)
                .expect("Run id must be discoverable")
                .to_string(),
            events,
        }
    }

    /// Add an event by type with attributes. Bundles both into a [HistoryEvent] with an id that is
    /// incremented on each call to add. Returns the id of the new event.
    pub fn add(&mut self, attribs: impl Into<Attributes>) -> i64 {
        let attribs: Attributes = attribs.into();
        self.build_and_push_event(attribs.event_type(), attribs);
        self.current_event_id
    }

    /// Adds an event to the history by type, with default attributes. Returns the id of the new
    /// event.
    pub fn add_by_type(&mut self, event_type: EventType) -> i64 {
        let attribs =
            default_attribs(event_type).expect("Couldn't make default attributes in test builder");
        self.add(attribs)
    }

    /// Adds the following events:
    /// ```text
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// ```
    pub fn add_full_wf_task(&mut self) {
        self.add_workflow_task_scheduled_and_started();
        self.add_workflow_task_completed();
    }

    pub fn add_workflow_task_scheduled_and_started(&mut self) {
        self.add_workflow_task_scheduled();
        self.add_workflow_task_started();
    }

    pub fn add_workflow_task_scheduled(&mut self) {
        self.workflow_task_scheduled_event_id = self.add_by_type(EventType::WorkflowTaskScheduled);
    }

    pub fn add_workflow_task_started(&mut self) {
        self.final_workflow_task_started_event_id = self.add(WorkflowTaskStartedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            history_size_bytes: ((self.events.len() + 1) * 10) as i64,
            ..Default::default()
        });
    }

    pub fn add_workflow_task_completed(&mut self) {
        let id = self.add(WorkflowTaskCompletedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        });
        self.previous_task_completed_id = id;
    }

    pub fn add_workflow_task_timed_out(&mut self) {
        let attrs = WorkflowTaskTimedOutEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskTimedOut, attrs.into());
    }

    pub fn add_workflow_execution_completed(&mut self) {
        let attrs = WorkflowExecutionCompletedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionCompleted, attrs.into());
    }

    pub fn add_workflow_execution_terminated(&mut self) {
        let attrs = WorkflowExecutionTerminatedEventAttributes {
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionTerminated, attrs.into());
    }

    pub fn add_workflow_execution_timed_out(&mut self) {
        let attrs = WorkflowExecutionTimedOutEventAttributes {
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionTimedOut, attrs.into());
    }

    pub fn add_workflow_execution_failed(&mut self) {
        let attrs = WorkflowExecutionFailedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionFailed, attrs.into());
    }

    pub fn add_continued_as_new(&mut self) {
        let attrs = WorkflowExecutionContinuedAsNewEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionContinuedAsNew, attrs.into());
    }

    pub fn add_cancel_requested(&mut self) {
        let attrs = WorkflowExecutionCancelRequestedEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionCancelRequested, attrs.into());
    }

    pub fn add_cancelled(&mut self) {
        let attrs = WorkflowExecutionCanceledEventAttributes::default();
        self.build_and_push_event(EventType::WorkflowExecutionCanceled, attrs.into());
    }

    pub fn add_activity_task_scheduled(&mut self, activity_id: impl Into<String>) -> i64 {
        self.add(ActivityTaskScheduledEventAttributes {
            activity_id: activity_id.into(),
            activity_type: Some(ActivityType {
                name: DEFAULT_ACTIVITY_TYPE.to_string(),
            }),
            ..Default::default()
        })
    }

    pub fn add_activity_task_started(&mut self, scheduled_event_id: i64) -> i64 {
        self.add(Attributes::ActivityTaskStartedEventAttributes(
            ActivityTaskStartedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ))
    }

    pub fn add_activity_task_completed(
        &mut self,
        scheduled_event_id: i64,
        started_event_id: i64,
        payload: Payload,
    ) {
        self.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            result: vec![payload].into_payloads(),
            ..Default::default()
        });
    }

    pub fn add_activity_task_cancel_requested(&mut self, scheduled_event_id: i64) {
        let attrs = ActivityTaskCancelRequestedEventAttributes {
            scheduled_event_id,
            workflow_task_completed_event_id: self.previous_task_completed_id,
        };
        self.build_and_push_event(EventType::ActivityTaskCancelRequested, attrs.into());
    }

    pub fn add_workflow_task_failed_with_failure(
        &mut self,
        cause: WorkflowTaskFailedCause,
        failure: Failure,
    ) {
        let attrs = WorkflowTaskFailedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            cause: cause.into(),
            failure: Some(failure),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskFailed, attrs.into());
    }

    pub fn add_workflow_task_failed_new_id(
        &mut self,
        cause: WorkflowTaskFailedCause,
        new_run_id: &str,
    ) {
        let attrs = WorkflowTaskFailedEventAttributes {
            scheduled_event_id: self.workflow_task_scheduled_event_id,
            cause: cause.into(),
            new_run_id: new_run_id.into(),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowTaskFailed, attrs.into());
    }

    pub fn add_timer_started(&mut self, timer_id: String) -> i64 {
        self.add(TimerStartedEventAttributes {
            timer_id,
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        })
    }

    pub fn add_timer_fired(&mut self, timer_started_evt_id: i64, timer_id: String) {
        self.add(TimerFiredEventAttributes {
            started_event_id: timer_started_evt_id,
            timer_id,
        });
    }

    pub fn add_we_signaled(&mut self, signal_name: &str, payloads: Vec<Payload>) {
        let attrs = WorkflowExecutionSignaledEventAttributes {
            signal_name: signal_name.to_string(),
            input: Some(Payloads { payloads }),
            ..Default::default()
        };
        self.build_and_push_event(EventType::WorkflowExecutionSignaled, attrs.into());
    }

    pub fn add_has_change_marker(&mut self, patch_id: &str, deprecated: bool) {
        let attrs = MarkerRecordedEventAttributes {
            marker_name: PATCH_MARKER_NAME.to_string(),
            details: build_has_change_marker_details(patch_id, deprecated).unwrap(),
            workflow_task_completed_event_id: self.previous_task_completed_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::MarkerRecorded, attrs.into());
    }

    pub fn add_local_activity_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Option<Payload>,
        failure: Option<Failure>,
        detail_mutator: impl FnOnce(&mut LocalActivityMarkerData),
    ) {
        let mut lamd = LocalActivityMarkerData {
            seq,
            attempt: 1,
            activity_id: activity_id.to_string(),
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            complete_time: None,
            backoff: None,
            original_schedule_time: None,
        };
        detail_mutator(&mut lamd);
        let attrs = MarkerRecordedEventAttributes {
            marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_string(),
            details: build_local_activity_marker_details(lamd, payload),
            workflow_task_completed_event_id: self.previous_task_completed_id,
            failure,
            ..Default::default()
        };
        self.build_and_push_event(EventType::MarkerRecorded, attrs.into());
    }

    pub fn add_local_activity_result_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Payload,
    ) {
        self.add_local_activity_marker(seq, activity_id, Some(payload), None, |_| {});
    }

    pub fn add_local_activity_result_marker_with_time(
        &mut self,
        seq: u32,
        activity_id: &str,
        payload: Payload,
        complete_time: Timestamp,
    ) {
        self.add_local_activity_marker(seq, activity_id, Some(payload), None, |d| {
            d.complete_time = Some(complete_time)
        });
    }

    pub fn add_local_activity_fail_marker(
        &mut self,
        seq: u32,
        activity_id: &str,
        failure: Failure,
    ) {
        self.add_local_activity_marker(seq, activity_id, None, Some(failure), |_| {});
    }

    pub fn add_local_activity_cancel_marker(&mut self, seq: u32, activity_id: &str) {
        self.add_local_activity_marker(
            seq,
            activity_id,
            None,
            Some(Failure {
                message: "cancelled bro".to_string(),
                source: "".to_string(),
                stack_trace: "".to_string(),
                cause: None,
                failure_info: Some(failure::FailureInfo::CanceledFailureInfo(
                    CanceledFailureInfo { details: None },
                )),
                encoded_attributes: Default::default(),
            }),
            |_| {},
        );
    }

    pub fn add_signal_wf(
        &mut self,
        signal_name: impl Into<String>,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> i64 {
        self.add(SignalExternalWorkflowExecutionInitiatedEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            workflow_execution: Some(WorkflowExecution {
                workflow_id: workflow_id.into(),
                run_id: run_id.into(),
            }),
            signal_name: signal_name.into(),
            ..Default::default()
        })
    }

    pub fn add_external_signal_completed(&mut self, initiated_id: i64) {
        let attrs = ExternalWorkflowExecutionSignaledEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(EventType::ExternalWorkflowExecutionSignaled, attrs.into());
    }

    pub fn add_external_signal_failed(&mut self, initiated_id: i64) {
        let attrs = SignalExternalWorkflowExecutionFailedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::SignalExternalWorkflowExecutionFailed,
            attrs.into(),
        );
    }

    pub fn add_cancel_external_wf(&mut self, execution: NamespacedWorkflowExecution) -> i64 {
        self.add(
            RequestCancelExternalWorkflowExecutionInitiatedEventAttributes {
                workflow_task_completed_event_id: self.previous_task_completed_id,
                namespace: execution.namespace,
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: execution.workflow_id,
                    run_id: execution.run_id,
                }),
                ..Default::default()
            },
        )
    }

    pub fn add_cancel_external_wf_completed(&mut self, initiated_id: i64) {
        let attrs = ExternalWorkflowExecutionCancelRequestedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::ExternalWorkflowExecutionCancelRequested,
            attrs.into(),
        );
    }

    pub fn add_cancel_external_wf_failed(&mut self, initiated_id: i64) {
        let attrs = RequestCancelExternalWorkflowExecutionFailedEventAttributes {
            initiated_event_id: initiated_id,
            ..Default::default()
        };
        self.build_and_push_event(
            EventType::RequestCancelExternalWorkflowExecutionFailed,
            attrs.into(),
        );
    }

    pub fn add_wfe_started_with_wft_timeout(&mut self, dur: Duration) {
        let mut wesattrs = default_wes_attribs();
        wesattrs.workflow_task_timeout = Some(dur.try_into().unwrap());
        self.add(wesattrs);
    }

    pub fn add_upsert_search_attrs_for_patch(&mut self, attribs: &[String]) {
        let mut indexed_fields = HashMap::new();
        indexed_fields.insert(
            "TemporalChangeVersion".to_string(),
            attribs.as_json_payload().unwrap(),
        );
        let attrs = UpsertWorkflowSearchAttributesEventAttributes {
            workflow_task_completed_event_id: self.previous_task_completed_id,
            search_attributes: Some(SearchAttributes { indexed_fields }),
        };
        self.build_and_push_event(EventType::UpsertWorkflowSearchAttributes, attrs.into());
    }

    pub fn add_update_accepted(
        &mut self,
        instance_id: impl Into<String>,
        update_name: impl Into<String>,
    ) -> i64 {
        let protocol_instance_id = instance_id.into();
        let last_wft_scheduled_id = self
            .events
            .iter()
            .rev()
            .find_map(|e| {
                if e.event_type() == EventType::WorkflowTaskScheduled {
                    Some(e.event_id)
                } else {
                    None
                }
            })
            .expect("Must have wft scheduled event");
        let attrs = WorkflowExecutionUpdateAcceptedEventAttributes {
            accepted_request_message_id: format!("{}/request", &protocol_instance_id),
            accepted_request_sequencing_event_id: last_wft_scheduled_id,
            accepted_request: Some(update::v1::Request {
                meta: Some(update::v1::Meta {
                    update_id: protocol_instance_id.clone(),
                    identity: "fake".to_string(),
                }),
                input: Some(update::v1::Input {
                    header: None,
                    name: update_name.into(),
                    args: None,
                }),
            }),
            protocol_instance_id,
        };
        self.build_and_push_event(EventType::WorkflowExecutionUpdateAccepted, attrs.into())
    }

    pub fn add_update_completed(&mut self, accepted_event_id: i64) {
        let attrs = WorkflowExecutionUpdateCompletedEventAttributes {
            meta: None,
            accepted_event_id,
            outcome: Some(update::v1::Outcome {
                value: Some(outcome::Value::Success(Payloads::default())),
            }),
        };
        self.build_and_push_event(EventType::WorkflowExecutionUpdateCompleted, attrs.into());
    }

    pub fn get_orig_run_id(&self) -> &str {
        &self.original_run_id
    }

    /// Iterates over the events in this builder to return a [HistoryInfo] including events up to
    /// the provided `to_wf_task_num`
    pub fn get_history_info(&self, to_wf_task_num: usize) -> Result<HistoryInfo, anyhow::Error> {
        HistoryInfo::new_from_history(&self.events.clone().into(), Some(to_wf_task_num))
    }

    /// Iterates over the events in this builder to return a [HistoryInfo] representing *all*
    /// events in the history
    pub fn get_full_history_info(&self) -> Result<HistoryInfo, anyhow::Error> {
        HistoryInfo::new_from_history(&self.events.clone().into(), None)
    }

    pub fn get_one_wft(&self, from_wft_number: usize) -> Result<HistoryInfo, anyhow::Error> {
        let mut histinfo =
            HistoryInfo::new_from_history(&self.events.clone().into(), Some(from_wft_number))?;
        histinfo.make_incremental();
        Ok(histinfo)
    }

    /// Return most recent wft start time or panic if unset
    pub fn wft_start_time(&self) -> Timestamp {
        self.events[(self.workflow_task_scheduled_event_id + 1) as usize]
            .event_time
            .unwrap()
    }

    /// Alter the workflow type of the history
    pub fn set_wf_type(&mut self, name: &str) {
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
            self.events.get_mut(0).and_then(|e| e.attributes.as_mut())
        {
            wes.workflow_type = Some(WorkflowType {
                name: name.to_string(),
            })
        }
    }

    /// Alter the input to the workflow
    pub fn set_wf_input(&mut self, input: impl Into<Payloads>) {
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
            self.events.get_mut(0).and_then(|e| e.attributes.as_mut())
        {
            wes.input = Some(input.into());
        }
    }

    /// Alter some specific event. You can easily craft nonsense histories this way, use carefully.
    pub fn modify_event(&mut self, event_id: i64, modifier: impl FnOnce(&mut HistoryEvent)) {
        let he = self
            .events
            .get_mut((event_id - 1) as usize)
            .expect("Event must be present");
        modifier(he);
    }

    /// Sets internal patches which should appear in the first WFT complete event
    pub fn set_flags_first_wft(&mut self, core: &[u32], lang: &[u32]) {
        Self::set_flags(self.events.iter_mut(), core, lang)
    }

    /// Sets internal patches which should appear in the most recent complete event
    pub fn set_flags_last_wft(&mut self, core: &[u32], lang: &[u32]) {
        Self::set_flags(self.events.iter_mut().rev(), core, lang)
    }

    /// Get the event ID of the most recently added event
    pub fn current_event_id(&self) -> i64 {
        self.current_event_id
    }

    /// Get mutable ref to the most recently added event
    pub fn last_event(&mut self) -> Option<&mut HistoryEvent> {
        self.events.last_mut()
    }

    fn set_flags<'a>(
        mut events: impl Iterator<Item = &'a mut HistoryEvent>,
        core: &[u32],
        lang: &[u32],
    ) {
        if let Some(first_attrs) = events.find_map(|e| {
            if let Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) = e.attributes.as_mut()
            {
                Some(a)
            } else {
                None
            }
        }) {
            let sdk_dat = first_attrs
                .sdk_metadata
                .get_or_insert_with(Default::default);
            sdk_dat.core_used_flags = core.to_vec();
            sdk_dat.lang_used_flags = lang.to_vec();
        }
    }

    fn build_and_push_event(&mut self, event_type: EventType, attribs: Attributes) -> i64 {
        self.current_event_id += 1;
        let evt = HistoryEvent {
            event_type: event_type as i32,
            event_id: self.current_event_id,
            event_time: Some(SystemTime::now().into()),
            attributes: Some(attribs),
            ..Default::default()
        };
        if let Some(Attributes::WorkflowExecutionStartedEventAttributes(
            WorkflowExecutionStartedEventAttributes {
                original_execution_run_id,
                ..
            },
        )) = &evt.attributes
        {
            self.original_run_id.clone_from(original_execution_run_id);
        };
        self.events.push(evt);
        self.current_event_id
    }
}

fn default_attribs(et: EventType) -> Result<Attributes> {
    Ok(match et {
        EventType::WorkflowExecutionStarted => default_wes_attribs().into(),
        EventType::WorkflowTaskScheduled => WorkflowTaskScheduledEventAttributes::default().into(),
        EventType::TimerStarted => TimerStartedEventAttributes::default().into(),
        _ => bail!("Don't know how to construct default attrs for {:?}", et),
    })
}

pub fn default_wes_attribs() -> WorkflowExecutionStartedEventAttributes {
    WorkflowExecutionStartedEventAttributes {
        original_execution_run_id: Uuid::new_v4().to_string(),
        workflow_type: Some(WorkflowType {
            name: DEFAULT_WORKFLOW_TYPE.to_owned(),
        }),
        workflow_task_timeout: Some(
            Duration::from_secs(5)
                .try_into()
                .expect("5 secs is a valid duration"),
        ),
        task_queue: Some(TaskQueue {
            name: "q".to_string(),
            kind: TaskQueueKind::Normal as i32,
            normal_name: "".to_string(),
        }),
        ..Default::default()
    }
}

pub fn default_act_sched() -> ScheduleActivity {
    ScheduleActivity {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        ..Default::default()
    }
}
