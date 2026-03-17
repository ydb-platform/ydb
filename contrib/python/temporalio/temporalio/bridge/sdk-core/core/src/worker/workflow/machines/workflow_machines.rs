mod local_acts;

use super::{
    Machines, NewMachineWithCommand, TemporalStateMachine,
    cancel_external_state_machine::new_external_cancel,
    cancel_workflow_state_machine::cancel_workflow,
    complete_workflow_state_machine::complete_workflow,
    continue_as_new_workflow_state_machine::continue_as_new,
    fail_workflow_state_machine::fail_workflow, local_activity_state_machine::new_local_activity,
    patch_state_machine::has_change, signal_external_state_machine::new_external_signal,
    timer_state_machine::new_timer, upsert_search_attributes_state_machine::upsert_search_attrs,
    workflow_machines::local_acts::LocalActivityData,
    workflow_task_state_machine::WorkflowTaskMachine,
};
use crate::{
    abstractions::dbg_panic,
    internal_flags::{CoreInternalFlags, InternalFlags},
    protosext::{
        CompleteLocalActivityData, HistoryEventExt, ValidScheduleLA,
        protocol_messages::{IncomingProtocolMessage, IncomingProtocolMessageBody},
    },
    telemetry::{VecDisplayer, metrics::MetricsContext},
    worker::{
        ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
        workflow::{
            CommandID, DrivenWorkflow, HistoryUpdate, InternalFlagsRef, LocalResolution,
            OutgoingJob, RunBasics, WFCommand, WFCommandVariant, WFMachinesError,
            WorkflowStartedInfo,
            history_update::NextWFT,
            machines::{
                HistEventData, activity_state_machine::ActivityMachine,
                child_workflow_state_machine::ChildWorkflowMachine,
                modify_workflow_properties_state_machine::modify_workflow_properties,
                nexus_operation_state_machine::NexusOperationMachine,
                patch_state_machine::VERSION_SEARCH_ATTR_KEY, update_state_machine::UpdateMachine,
                upsert_search_attributes_state_machine::upsert_search_attrs_internal,
            },
        },
    },
};
use anyhow::Context;
use siphasher::sip::SipHasher13;
use slotmap::{SlotMap, SparseSecondaryMap};
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    convert::TryInto,
    hash::{Hash, Hasher},
    iter::Peekable,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_api::worker::{WorkerConfig, WorkerDeploymentVersion};
use temporal_sdk_core_protos::{
    coresdk::{
        common::{NamespacedWorkflowExecution, VersioningIntent},
        workflow_activation,
        workflow_activation::{
            NotifyHasPatch, UpdateRandomSeed, WorkflowActivation, workflow_activation_job,
        },
        workflow_commands::ContinueAsNewWorkflowExecution,
    },
    temporal::api::{
        command::v1::{
            Command as ProtoCommand, CommandAttributesExt, command::Attributes as ProtoCmdAttrs,
        },
        enums::v1::EventType,
        history::v1::{HistoryEvent, history_event},
        protocol::v1::{Message as ProtocolMessage, message::SequencingId},
        sdk::v1::{UserMetadata, WorkflowTaskCompletedMetadata},
    },
};

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

slotmap::new_key_type! { struct MachineKey; }
/// Handles all the logic for driving a workflow. It orchestrates many state machines that together
/// comprise the logic of an executing workflow. One instance will exist per currently executing
/// (or cached) workflow on the worker.
pub(crate) struct WorkflowMachines {
    /// The last recorded history we received from the server for this workflow run. This must be
    /// kept because the lang side polls & completes for every workflow task, but we do not need
    /// to poll the server that often during replay.
    last_history_from_server: HistoryUpdate,
    /// Protocol messages that have yet to be processed for the current WFT.
    protocol_msgs: Vec<IncomingProtocolMessage>,
    /// EventId of the last handled WorkflowTaskStarted event
    current_started_event_id: i64,
    /// The event id of the next workflow task started event that the machines need to process.
    /// Eventually, this number should reach the started id in the latest history update, but
    /// we must incrementally apply the history while communicating with lang.
    next_started_event_id: i64,
    /// The event id of the most recent event processed. It's possible in some situations (ex legacy
    /// queries) to receive a history with no new workflow tasks. If the last history we processed
    /// also had no new tasks, we need a way to know not to apply the same events over again.
    pub(crate) last_processed_event: i64,
    /// True if the workflow is replaying from history
    pub(crate) replaying: bool,
    /// Workflow identifier
    pub(crate) workflow_id: String,
    /// Workflow type identifier. (Function name, class, etc)
    pub(crate) workflow_type: String,
    /// Identifies the current run
    pub(crate) run_id: String,
    /// Is set to true once we've seen the final event in workflow history, to avoid accidentally
    /// re-applying the final workflow task.
    pub(crate) have_seen_terminal_event: bool,
    /// The time the workflow execution began, as told by the WEStarted event
    workflow_start_time: Option<SystemTime>,
    /// The time the workflow execution finished, as determined by when the machines handled
    /// a terminal workflow command. If this is `Some`, you know the workflow is ended.
    workflow_end_time: Option<SystemTime>,
    /// The WFT start time if it has been established
    wft_start_time: Option<SystemTime>,
    /// The current workflow time if it has been established. This may differ from the WFT start
    /// time since local activities may advance the clock
    current_wf_time: Option<SystemTime>,
    /// The internal flags which have been seen so far during this run's execution and thus are
    /// usable during replay.
    observed_internal_flags: InternalFlagsRef,
    /// Set on each WFT started event, the most recent size of history in bytes
    history_size_bytes: u64,
    /// Set on each WFT started event
    continue_as_new_suggested: bool,
    /// Set if the current WFT is already complete and that completion event had legacy build-id
    /// or a deployment version in it. Will use an empty deployment name if it's legacy build-id.
    current_wft_deployment_info: Option<WorkerDeploymentVersion>,

    all_machines: SlotMap<MachineKey, Machines>,
    /// If a machine key is in this map, that machine was created internally by core, not as a
    /// command from lang.
    machine_is_core_created: SparseSecondaryMap<MachineKey, ()>,
    /// A mapping for accessing machines associated to a particular event, where the key is the id
    /// of the initiating event for that machine.
    machines_by_event_id: HashMap<i64, MachineKey>,
    /// A mapping for accessing machines that were created as a result of protocol messages. The
    /// key is the protocol's instance id.
    machines_by_protocol_instance_id: HashMap<String, MachineKey>,
    /// Maps command ids as created by workflow authors to their associated machines.
    id_to_machine: HashMap<CommandID, MachineKey>,

    /// Queued commands which have been produced by machines and await processing / being sent to
    /// the server.
    commands: VecDeque<CommandAndMachine>,
    /// Commands generated by the currently processing workflow task, which will eventually be
    /// transferred to `commands` (and hence eventually sent to the server)
    current_wf_task_commands: VecDeque<CommandAndMachine>,
    /// Messages generated while processing the current workflow task, which will be sent in the
    /// next WFT response. Keyed by message id.
    message_outbox: VecDeque<ProtocolMessage>,

    /// Information about patch markers we have already seen while replaying history
    encountered_patch_markers: HashMap<String, ChangeInfo>,

    /// Contains extra local-activity related data
    local_activity_data: LocalActivityData,

    /// The workflow that is being driven by this instance of the machines
    drive_me: DrivenWorkflow,

    /// Metrics context
    pub(crate) metrics: MetricsContext,
    worker_config: Arc<WorkerConfig>,
}

#[derive(Debug, derive_more::Display)]
#[display("Cmd&Machine({command})")]
struct CommandAndMachine {
    command: MachineAssociatedCommand,
    machine: MachineKey,
}

#[derive(Debug, derive_more::Display)]
enum MachineAssociatedCommand {
    Real(Box<ProtoCommand>),
    #[display("FakeLocalActivityMarker({_0})")]
    FakeLocalActivityMarker(u32),
}

#[derive(Debug, Clone, Copy)]
struct ChangeInfo {
    created_command: bool,
}

/// Returned by [TemporalStateMachine]s when handling events
#[derive(Debug, derive_more::Display)]
#[must_use]
#[allow(clippy::large_enum_variant)]
pub(super) enum MachineResponse {
    #[display("PushWFJob({_0})")]
    PushWFJob(OutgoingJob),

    /// Pushes a new command into the list that will be sent to server once we respond with the
    /// workflow task completion
    IssueNewCommand(ProtoCommand),
    /// Pushes a new protocol message into the list that will be sent to server once we respond with
    /// the workflow task completion
    IssueNewMessage(ProtocolMessage),
    /// The machine requests the creation of another *different* machine. This acts as if lang
    /// had replied to the activation with a command, but we use a special set of IDs to avoid
    /// collisions.
    #[display("NewCoreOriginatedCommand({_0:?})")]
    NewCoreOriginatedCommand(ProtoCmdAttrs),
    #[display("IssueFakeLocalActivityMarker({_0})")]
    IssueFakeLocalActivityMarker(u32),
    #[display("TriggerWFTaskStarted")]
    TriggerWFTaskStarted {
        task_started_event_id: i64,
        time: SystemTime,
    },
    #[display("UpdateRunIdOnWorkflowReset({run_id})")]
    UpdateRunIdOnWorkflowReset { run_id: String },

    /// Queue a local activity to be processed by the worker
    #[display("QueueLocalActivity")]
    QueueLocalActivity(ValidScheduleLA),
    /// Request cancellation of an executing local activity
    #[display("RequestCancelLocalActivity({_0})")]
    RequestCancelLocalActivity(u32),
    /// Indicates we are abandoning the indicated LA, so we can remove it from "outstanding" LAs
    /// and we will not try to WFT heartbeat because of it.
    #[display("AbandonLocalActivity({_0:?})")]
    AbandonLocalActivity(u32),

    /// Set the workflow time to the provided time
    #[display("UpdateWFTime({_0:?})")]
    UpdateWFTime(Option<SystemTime>),
}

impl<T> From<T> for MachineResponse
where
    T: Into<workflow_activation_job::Variant>,
{
    fn from(v: T) -> Self {
        Self::PushWFJob(v.into().into())
    }
}

/// Helper macro for invoking cancels on machines
macro_rules! cancel_machine {
    ($self:expr, $cmd_id:expr, $machine_variant:ident, $cancel_method:ident $(, $args:expr )* $(,)?) => {{
        let m_key = $self.get_machine_key($cmd_id)?;
        let machine = if let Machines::$machine_variant(m) = $self.machine_mut(m_key) {
            m
        } else {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Machine was not a {} when it should have been during cancellation: {:?}",
                stringify!($machine_variant),
                $cmd_id
            )));
        };
        let machine_resps = machine.$cancel_method($($args),*)?;
        $self.process_machine_responses(m_key, machine_resps)?
    }};
}

impl WorkflowMachines {
    pub(crate) fn new(basics: RunBasics, driven_wf: DrivenWorkflow) -> Self {
        let replaying = basics.history.previous_wft_started_id > 0;
        let mut observed_internal_flags = InternalFlags::new(
            basics.capabilities,
            basics.sdk_name.to_owned(),
            basics.sdk_version.to_owned(),
        );
        // Peek ahead to determine used flags in the first WFT.
        if let Some(attrs) = basics.history.peek_next_wft_completed(0) {
            observed_internal_flags.add_from_complete(attrs);
        };
        Self {
            last_history_from_server: basics.history,
            protocol_msgs: vec![],
            workflow_id: basics.workflow_id,
            workflow_type: basics.workflow_type,
            run_id: basics.run_id,
            drive_me: driven_wf,
            replaying,
            metrics: basics.metrics,
            // In an ideal world one could say ..Default::default() here and it'd still work.
            current_started_event_id: 0,
            next_started_event_id: 0,
            last_processed_event: 0,
            workflow_start_time: None,
            workflow_end_time: None,
            wft_start_time: None,
            current_wf_time: None,
            observed_internal_flags: Rc::new(RefCell::new(observed_internal_flags)),
            history_size_bytes: 0,
            continue_as_new_suggested: false,
            current_wft_deployment_info: None,
            all_machines: Default::default(),
            machine_is_core_created: Default::default(),
            machines_by_event_id: Default::default(),
            machines_by_protocol_instance_id: Default::default(),
            id_to_machine: Default::default(),
            commands: Default::default(),
            current_wf_task_commands: Default::default(),
            message_outbox: Default::default(),
            encountered_patch_markers: Default::default(),
            local_activity_data: LocalActivityData::default(),
            have_seen_terminal_event: false,
            worker_config: basics.worker_config,
        }
    }

    /// Returns true if workflow has seen a terminal command
    pub(crate) const fn workflow_is_finished(&self) -> bool {
        self.workflow_end_time.is_some()
    }

    /// Returns the total time it took to execute the workflow. Returns `None` if workflow is
    /// incomplete, or time went backwards.
    pub(crate) fn total_runtime(&self) -> Option<Duration> {
        self.workflow_start_time
            .zip(self.workflow_end_time)
            .and_then(|(st, et)| et.duration_since(st).ok())
    }

    /// Must be called every time a new WFT is received
    pub(crate) fn new_work_from_server(
        &mut self,
        update: HistoryUpdate,
        protocol_messages: Vec<IncomingProtocolMessage>,
    ) -> Result<()> {
        if !self.protocol_msgs.is_empty() {
            dbg_panic!("There are unprocessed protocol messages while receiving new work");
        }
        self.protocol_msgs = protocol_messages;
        self.new_history_from_server(update)?;
        Ok(())
    }

    pub(crate) fn new_history_from_server(&mut self, update: HistoryUpdate) -> Result<()> {
        self.last_history_from_server = update;
        self.replaying = self.last_history_from_server.previous_wft_started_id > 0;
        self.apply_next_wft_from_history()?;
        Ok(())
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    pub(crate) fn local_resolution(&mut self, resolution: LocalResolution) -> Result<bool> {
        let mut result_important = true;
        match resolution {
            LocalResolution::LocalActivity(LocalActivityResolution {
                seq,
                result,
                runtime,
                attempt,
                backoff,
                original_schedule_time,
            }) => {
                let act_id = CommandID::LocalActivity(seq);
                let mk = self.get_machine_key(act_id)?;
                let mach = self.machine_mut(mk);
                if let Machines::LocalActivityMachine(ref mut lam) = *mach {
                    let resps =
                        lam.try_resolve(result, runtime, attempt, backoff, original_schedule_time)?;
                    if resps.is_empty() {
                        result_important = false;
                    }
                    self.process_machine_responses(mk, resps)?;
                } else {
                    return Err(WFMachinesError::Nondeterminism(format!(
                        "Command matching activity with seq num {seq} existed but was not a \
                        local activity!"
                    )));
                }
                self.local_activity_data.done_executing(seq);
            }
        }
        Ok(result_important)
    }

    /// Drain all queued local activities that need executing or cancellation
    pub(crate) fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.local_activity_data
            .take_all_reqs(&self.workflow_type, &self.workflow_id, &self.run_id)
    }

    /// Returns the number of local activities we know we need to execute but have not yet finished
    pub(crate) fn outstanding_local_activity_count(&self) -> usize {
        self.local_activity_data.outstanding_la_count()
    }

    /// Returns start info for the workflow if it has started
    pub(crate) fn get_started_info(&self) -> Option<&WorkflowStartedInfo> {
        self.drive_me.get_started_info()
    }

    pub(crate) fn get_last_wft_started_id(&self) -> i64 {
        self.current_started_event_id
    }

    pub(crate) fn prepare_for_wft_response(&mut self) -> MachinesWFTResponseContent<'_> {
        MachinesWFTResponseContent {
            replaying: self.replaying,
            has_pending_jobs: self.has_pending_jobs(),
            have_seen_terminal_event: self.have_seen_terminal_event,
            have_pending_la_resolutions: self.has_pending_la_resolutions(),
            me: self,
        }
    }

    /// Fetches commands which are ready for processing from the state machines, generally to be
    /// sent off to the server. They are not removed from the internal queue, that happens when
    /// corresponding history events from the server are being handled.
    pub(crate) fn get_commands(&self) -> impl Iterator<Item = ProtoCommand> + '_ {
        // Since we're about to write a WFT, record any internal flags we know about which aren't
        // already recorded.
        (*self.observed_internal_flags)
            .borrow_mut()
            .write_all_known();
        self.commands.iter().filter_map(|c| {
            if !self.machine(c.machine).is_final_state() {
                match &c.command {
                    MachineAssociatedCommand::Real(cmd) => Some((**cmd).clone()),
                    MachineAssociatedCommand::FakeLocalActivityMarker(_) => None,
                }
            } else {
                None
            }
        })
    }

    /// Returns the next activation that needs to be performed by the lang sdk. Things like unblock
    /// timer, etc. This does *not* cause any advancement of the state machines, it merely drains
    /// from the outgoing queue of activation jobs.
    ///
    /// The job list may be empty, in which case it is expected the caller handles what to do in a
    /// "no work" situation. Possibly, it may know about some work the machines don't, like queries.
    pub(crate) fn get_wf_activation(&mut self) -> WorkflowActivation {
        let jobs = self.drive_me.drain_jobs();
        // Even though technically we may have satisfied all the criteria to be done with replay,
        // query only activations are always "replaying" to keep things sane.
        let all_query = jobs.iter().all(|j| {
            matches!(
                j.variant,
                Some(workflow_activation_job::Variant::QueryWorkflow(_))
            )
        });
        let is_replaying = self.replaying || all_query;
        let deployment_version_for_current_task = if is_replaying {
            self.current_wft_deployment_info.clone()
        } else {
            self.current_wft_deployment_info = self.worker_config.computed_deployment_version();
            self.current_wft_deployment_info.clone()
        };
        WorkflowActivation {
            timestamp: self.current_wf_time.map(Into::into),
            is_replaying,
            run_id: self.run_id.clone(),
            history_length: self.last_processed_event as u32,
            jobs,
            available_internal_flags: (*self.observed_internal_flags)
                .borrow()
                .all_lang()
                .collect(),
            history_size_bytes: self.history_size_bytes,
            continue_as_new_suggested: self.continue_as_new_suggested,
            deployment_version_for_current_task: deployment_version_for_current_task
                .map(Into::into),
        }
    }

    pub(crate) fn has_pending_jobs(&self) -> bool {
        !self.drive_me.peek_pending_jobs().is_empty()
    }

    pub(crate) fn has_pending_la_resolutions(&self) -> bool {
        self.drive_me
            .peek_pending_jobs()
            .iter()
            .any(|v| v.variant.is_local_activity_resolution())
    }

    pub(crate) fn get_metadata_for_wft_complete(&mut self) -> WorkflowTaskCompletedMetadata {
        // If this worker has a build ID and we're completing the task, we want to say our ID is the
        // current build ID, so that if we get a query before any new history, we properly can
        // report that our ID was the one used for the completion.
        self.current_wft_deployment_info = self.worker_config.computed_deployment_version();
        (*self.observed_internal_flags)
            .borrow_mut()
            .gather_for_wft_complete()
    }

    pub(crate) fn add_lang_used_flags(&self, flags: Vec<u32>) {
        (*self.observed_internal_flags)
            .borrow_mut()
            .add_lang_used(flags);
    }

    pub(crate) fn try_use_flag(&self, flag: CoreInternalFlags, should_record: bool) -> bool {
        self.observed_internal_flags
            .borrow_mut()
            .try_use(flag, should_record)
    }

    /// Undo a speculative workflow task by resetting to a certain WFT Started ID. This can happen
    /// when an update request is rejected.
    pub(crate) fn reset_last_started_id(&mut self, id: i64) {
        debug!("Resetting back to event id {} due to speculative WFT", id);
        self.current_started_event_id = id;
        // We must reset the last event we "processed" to be after the last WFT we really completed
        // + any command events (since the SDK "processed" those when it emitted the commands). This
        // is also equal to what we just processed in the speculative task, minus two, since we
        // would've just handled the most recent WFT started event, and we need to drop that & the
        // schedule event just before it.
        self.last_processed_event -= 2;
        // Then, we have to drop any state machines (which should only be one workflow task machine)
        // we may have created when servicing the speculative task.
        let remove_these: Vec<_> = self
            .machines_by_event_id
            .extract_if(|mid, _| *mid > self.last_processed_event)
            .collect();
        for (_, mkey) in remove_these {
            self.all_machines.remove(mkey);
        }
    }

    /// Iterate the state machines, which consists of grabbing any pending outgoing commands from
    /// the workflow code, handling them, and preparing them to be sent off to the server.
    pub(crate) fn iterate_machines(&mut self) -> Result<()> {
        let results = self.drive_me.fetch_workflow_iteration_output();
        self.handle_driven_results(results)?;
        self.prepare_commands()?;
        if self.workflow_is_finished()
            && let Some(rt) = self.total_runtime()
        {
            self.metrics.wf_e2e_latency(rt);
        }
        Ok(())
    }

    /// Returns true if machines are ready to apply the next WFT sequence, false if events will need
    /// to be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn ready_to_apply_next_wft(&self) -> bool {
        self.last_history_from_server
            .can_take_next_wft_sequence(self.current_started_event_id)
    }

    /// Apply the next (unapplied) entire workflow task from history to these machines. Will replay
    /// any events that need to be replayed until caught up to the newest WFT.
    pub(crate) fn apply_next_wft_from_history(&mut self) -> Result<usize> {
        // If we have already seen the terminal event for the entire workflow in a previous WFT,
        // then we don't need to do anything here, and in fact we need to avoid re-applying the
        // final WFT.
        if self.have_seen_terminal_event {
            // Replay clearly counts as done now, since we return here and never do anything else.
            self.replaying = false;
            return Ok(0);
        }

        let last_handled_wft_started_id = self.current_started_event_id;
        let (events, has_final_event) = match self
            .last_history_from_server
            .take_next_wft_sequence(last_handled_wft_started_id)
        {
            NextWFT::ReplayOver => (vec![], true),
            NextWFT::WFT(mut evts, has_final_event) => {
                // Do not re-process events we have already processed
                evts.retain(|e| e.event_id > self.last_processed_event);
                (evts, has_final_event)
            }
            NextWFT::NeedFetch => {
                return Err(WFMachinesError::Fatal(
                    "Need to fetch history events to continue applying workflow task, but this \
                     should be prevented ahead of time! This is a Core SDK bug."
                        .to_string(),
                ));
            }
        };
        let num_events_to_process = events.len();

        // Process any WFT completed events in the next sequence, as well as peek ahead to the
        // subsequent one to properly apply flags & any other data. Macro used to avoid self
        // double-borrow.
        macro_rules! apply_wft_complete_data {
            ($me:expr, $wtc:expr) => {{
                (*$me.observed_internal_flags)
                    .borrow_mut()
                    .add_from_complete($wtc);
                let mut combined_ver = WorkerDeploymentVersion {
                    deployment_name: "".to_string(),
                    build_id: "".to_string(),
                };
                #[allow(deprecated)]
                if let Some(bid) = $wtc.worker_version.as_ref().map(|wv| &wv.build_id) {
                    combined_ver.build_id = bid.to_string();
                }
                if !$wtc.worker_deployment_name.is_empty() {
                    combined_ver.deployment_name = $wtc.worker_deployment_name.clone();
                }
                #[allow(deprecated)]
                if !$wtc.worker_deployment_version.is_empty() {
                    if let Ok(ver) = $wtc.worker_deployment_version.parse() {
                        combined_ver = ver;
                    }
                }
                if let Some(dv) = $wtc.deployment_version.as_ref() {
                    combined_ver = dv.clone().into();
                }
                if !combined_ver.is_empty() {
                    $me.current_wft_deployment_info = Some(combined_ver);
                }
            }};
        }
        let mut peeked_events = events.iter().peekable();
        while let Some(event) = peeked_events.next() {
            if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(ref wtc)) =
                event.attributes
            {
                apply_wft_complete_data!(self, wtc);
            }
            if peeked_events.peek().is_none()
                && let Some(wtc) = self
                    .last_history_from_server
                    .peek_next_wft_completed(event.event_id)
            {
                apply_wft_complete_data!(self, wtc);
            }
        }

        // We're caught up on replay if there are no new events to process
        if events.is_empty() {
            self.replaying = false;
        }
        let replay_start = Instant::now();

        if let Some(last_event) = events.last()
            && last_event.event_type == EventType::WorkflowTaskStarted as i32
        {
            self.next_started_event_id = last_event.event_id;
        }

        let mut update_admitted_event_messages = HashMap::<String, IncomingProtocolMessage>::new();
        let mut do_handle_event = true;
        let mut history = events.into_iter().peekable();
        while let Some(event) = history.next() {
            let eid = event.event_id;
            if eid != self.last_processed_event + 1 {
                return Err(WFMachinesError::Fatal(format!(
                    "History is out of order. Last processed event: {}, event id: {}",
                    self.last_processed_event, eid
                )));
            }
            let next_event = history.peek();

            // This definition of replaying here is that we are no longer replaying as soon as we
            // see new events that have never been seen or produced by the SDK.
            //
            // Specifically, replay ends once we have seen any non-command event (IE: events that
            // aren't a result of something we produced in the SDK) on a WFT which has the final
            // event in history (meaning we are processing the most recent WFT and there are no
            // more subsequent WFTs). WFT Completed in this case does not count as a non-command
            // event, because that will typically show up as the first event in an incremental
            // history, and we want to ignore it and its associated commands since we "produced"
            // them.
            if self.replaying
                && has_final_event
                && eid > self.last_history_from_server.previous_wft_started_id
                && event.event_type() != EventType::WorkflowTaskCompleted
                && !event.is_command_event()
            {
                // Replay is finished
                self.replaying = false;
            }

            if matches!(
                event.attributes,
                Some(history_event::Attributes::WorkflowExecutionUpdateAdmittedEventAttributes(_)),
            ) {
                // The server has sent a durable update admitted event: create the message that
                // would have been sent for a non-durable update request message.
                let msg = IncomingProtocolMessage::try_from(&event).context(
                    "Failed to create protocol message from WorkflowExecutionUpdateAdmittedEvent",
                )?;
                if self.replaying {
                    // Stash the message for use if the update request is accepted.
                    update_admitted_event_messages.insert(msg.protocol_instance_id.clone(), msg);
                } else {
                    // Use the message now.
                    self.protocol_msgs.push(msg);
                }
                do_handle_event = false;
            }

            // Process any messages that should be processed before the event we're about to handle
            let for_event_id = eid - 1;
            // Another thing to replace when `drain_filter` exists
            let mut processable_msgs = vec![];
            self.protocol_msgs = std::mem::take(&mut self.protocol_msgs)
                .into_iter()
                .filter_map(|x| {
                    if x.processable_after_event_id()
                        .is_some_and(|eid| eid <= for_event_id)
                    {
                        processable_msgs.push(x);
                        None
                    } else {
                        Some(x)
                    }
                })
                .collect();
            for msg in processable_msgs {
                self.handle_protocol_message(msg)?;
            }

            if do_handle_event {
                let eho = self.handle_event(
                    HistEventData {
                        event,
                        replaying: self.replaying,
                        current_task_is_last_in_history: has_final_event,
                    },
                    next_event,
                )?;
                if matches!(
                    eho,
                    EventHandlingOutcome::SkipEvent {
                        skip_next_event: true
                    }
                ) {
                    do_handle_event = false;
                }
            } else {
                do_handle_event = true;
            }
            self.last_processed_event = eid;
        }

        // Needed to delay mutation of self until after we've iterated over peeked events.
        enum DelayedAction {
            WakeLa(MachineKey, Box<CompleteLocalActivityData>),
            ProtocolMessage(IncomingProtocolMessage),
        }
        let mut delayed_actions = vec![];
        // Scan through to the next WFT, searching for any patch / la markers, so that we can
        // pre-resolve them. This lookahead is necessary because we need these things to be already
        // resolved in the same activations they would have been resolved in during initial
        // execution. For example: If a workflow asks to run an LA and then waits on it, we will
        // write the completion marker at the end of that WFT (as a command). So, upon replay,
        // we need to lookahead and see that that LA is in fact resolved, so that we don't decide
        // to execute it anew when lang says it wants to run it.
        //
        // Alternatively, lookahead can seemingly be avoided if we were to consider the commands
        // that follow a WFT to be _part of_ that wft rather than the next one. That change might
        // make sense to do, and maybe simplifies things slightly, but is a substantial alteration.
        for e in self
            .last_history_from_server
            .peek_next_wft_sequence(last_handled_wft_started_id)
        {
            if let Some((patch_id, _)) = e.get_patch_marker_details() {
                self.encountered_patch_markers.insert(
                    patch_id.clone(),
                    ChangeInfo {
                        created_command: false,
                    },
                );
                // Found a patch marker
                self.drive_me.send_job(
                    workflow_activation_job::Variant::NotifyHasPatch(NotifyHasPatch { patch_id })
                        .into(),
                );
            } else if e.is_local_activity_marker() {
                if let Some(la_dat) = e.clone().into_local_activity_marker_details() {
                    if let Ok(mk) =
                        self.get_machine_key(CommandID::LocalActivity(la_dat.marker_dat.seq))
                    {
                        delayed_actions.push(DelayedAction::WakeLa(mk, Box::new(la_dat)));
                    } else {
                        self.local_activity_data.insert_peeked_marker(la_dat);
                    }
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Local activity marker was unparsable: {e:?}"
                    )));
                }
            } else if let Some(
                history_event::Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(ref atts),
            ) = e.attributes
            {
                // We've encountered an UpdateAccepted event during replay: pretend that we received
                // the message we would have when receiving an update request under not-replay. If
                // this event was preceded by an UpdateAdmitted event, then use the message that we
                // created when we encountered that.
                delayed_actions.push(DelayedAction::ProtocolMessage(
                    update_admitted_event_messages
                        .remove(&atts.protocol_instance_id)
                        .map_or_else(|| e.try_into().context(
                            "Failed to create protocol message from WorkflowExecutionUpdateAcceptedEvent",
                        ), Ok)?,
                ));
            }
        }
        for action in delayed_actions {
            match action {
                DelayedAction::WakeLa(mk, la_dat) => {
                    let mach = self.machine_mut(mk);
                    if let Machines::LocalActivityMachine(ref mut lam) = *mach {
                        if lam.will_accept_resolve_marker() {
                            let resps = lam.try_resolve_with_dat((*la_dat).into())?;
                            self.process_machine_responses(mk, resps)?;
                        } else {
                            self.local_activity_data.insert_peeked_marker(*la_dat);
                        }
                    }
                }
                DelayedAction::ProtocolMessage(pm) => {
                    self.handle_protocol_message(pm)?;
                }
            }
        }

        if !self.replaying {
            self.metrics.wf_task_replay_latency(replay_start.elapsed());
        }

        Ok(num_events_to_process)
    }

    /// Handle a single event from the workflow history.
    ///
    /// This function will attempt to apply the event to the workflow state machines. If there is
    /// not a matching machine for the event, a nondeterminism error is returned. Otherwise, the
    /// event is applied to the machine, which may also return a nondeterminism error if the machine
    /// does not match the expected type. A fatal error may be returned if the machine is in an
    /// invalid state.
    #[instrument(skip(self, event_dat), fields(event=%event_dat))]
    fn handle_event(
        &mut self,
        event_dat: HistEventData,
        next_event: Option<&HistoryEvent>,
    ) -> Result<EventHandlingOutcome> {
        let event = &event_dat.event;
        if event.is_final_wf_execution_event() {
            self.have_seen_terminal_event = true;
        }
        if event.is_ignorable() {
            return Ok(EventHandlingOutcome::Normal);
        }
        if matches!(
            event.event_type(),
            EventType::WorkflowExecutionTerminated | EventType::WorkflowExecutionTimedOut
        ) {
            let are_more_events =
                next_event.is_some() || !event_dat.current_task_is_last_in_history;
            return if are_more_events {
                Err(WFMachinesError::Fatal(
                    "Machines were fed a history which has an event after workflow execution was \
                     terminated!"
                        .to_string(),
                ))
            } else {
                Ok(EventHandlingOutcome::Normal)
            };
        }
        if event.event_type() == EventType::Unspecified || event.attributes.is_none() {
            return if !event.worker_may_ignore {
                Err(WFMachinesError::Fatal(format!(
                    "Event type is unspecified! This history is invalid. Event detail: {event:?}"
                )))
            } else {
                debug!("Event is ignorable");
                Ok(EventHandlingOutcome::SkipEvent {
                    skip_next_event: false,
                })
            };
        }

        if event.is_command_event() {
            return self.handle_command_event(event_dat, next_event);
        }

        if let Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(ref attrs)) =
            event.attributes
        {
            self.history_size_bytes = u64::try_from(attrs.history_size_bytes).unwrap_or_default();
            self.continue_as_new_suggested = attrs.suggest_continue_as_new;
        }

        if let Some(initial_cmd_id) = event.get_initial_command_event_id() {
            let mkey = self
                .machines_by_event_id
                .get(&initial_cmd_id)
                .ok_or_else(|| {
                    WFMachinesError::Nondeterminism(format!(
                        "During event handling, this event had an initial command ID but we \
                         could not find a matching command for it: {event:?}"
                    ))
                })?;
            self.submachine_handle_event(*mkey, event_dat)?;
        } else {
            self.handle_non_stateful_event(event_dat)?;
        };

        Ok(EventHandlingOutcome::Normal)
    }

    /// A command event is an event which is generated from a command emitted as a result of
    /// performing a workflow task. Each command has a corresponding event. For example
    /// ScheduleActivityTaskCommand is recorded to the history as ActivityTaskScheduledEvent.
    ///
    /// Command events always follow WorkflowTaskCompletedEvent.
    ///
    /// The handling consists of verifying that the next command in the commands queue is associated
    /// with a state machine, which is then notified about the event and the command is removed from
    /// the commands queue.
    fn handle_command_event(
        &mut self,
        event_dat: HistEventData,
        next_event: Option<&HistoryEvent>,
    ) -> Result<EventHandlingOutcome> {
        let event = &event_dat.event;

        if event.is_local_activity_marker() {
            let deets = event.extract_local_activity_marker_data().ok_or_else(|| {
                WFMachinesError::Fatal(format!("Local activity marker was unparsable: {event:?}"))
            })?;
            let cmdid = CommandID::LocalActivity(deets.seq);
            let mkey = self.get_machine_key(cmdid)?;
            if let Machines::LocalActivityMachine(lam) = self.machine(mkey) {
                if lam.marker_should_get_special_handling()? {
                    self.submachine_handle_event(mkey, event_dat)?;
                    return Ok(EventHandlingOutcome::Normal);
                }
            } else {
                return Err(WFMachinesError::Fatal(format!(
                    "Encountered local activity marker but the associated machine was of the \
                     wrong type! {event:?}"
                )));
            }
        }

        let event_id = event.event_id;

        let consumed_cmd = loop {
            let maybe_machine = self.commands.front().map(|mk| self.machine(mk.machine));
            match patch_marker_handling(event, maybe_machine, next_event)? {
                EventHandlingOutcome::SkipCommand => {
                    self.commands.pop_front();
                    continue;
                }
                eho @ EventHandlingOutcome::SkipEvent { .. } => return Ok(eho),
                EventHandlingOutcome::Normal => {}
            }

            let maybe_command = self.commands.pop_front();
            let command = if let Some(c) = maybe_command {
                c
            } else {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "No command scheduled for event {event}"
                )));
            };

            let canceled_before_sent = self
                .machine(command.machine)
                .was_cancelled_before_sent_to_server();

            if !canceled_before_sent {
                // Feed the machine the event
                self.submachine_handle_event(command.machine, event_dat)?;
                break command;
            }
        };

        if !self.machine(consumed_cmd.machine).is_final_state() {
            self.machines_by_event_id
                .insert(event_id, consumed_cmd.machine);
        }

        Ok(EventHandlingOutcome::Normal)
    }

    fn handle_non_stateful_event(&mut self, event_dat: HistEventData) -> Result<()> {
        trace!(event = %event_dat.event, "handling non-stateful event");
        let event_id = event_dat.event.event_id;
        match EventType::try_from(event_dat.event.event_type) {
            Ok(EventType::WorkflowExecutionStarted) => {
                if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
                    attrs,
                )) = event_dat.event.attributes
                {
                    if let Some(st) = event_dat.event.event_time {
                        let as_systime: SystemTime = st.try_into()?;
                        self.workflow_start_time = Some(as_systime);
                        // Set the workflow time to be the event time of the first event, so that
                        // if there is a query issued before first WFT started event, there is some
                        // workflow time set.
                        self.set_current_time(as_systime);
                    }
                    // Notify the lang sdk that it's time to kick off a workflow
                    self.drive_me.start(
                        self.workflow_id.clone(),
                        str_to_randomness_seed(&attrs.original_execution_run_id),
                        event_dat.event.event_time.unwrap_or_default(),
                        attrs,
                    );
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "WorkflowExecutionStarted event did not have appropriate attributes: {event_dat}"
                    )));
                }
            }
            Ok(EventType::WorkflowTaskScheduled) => {
                let wf_task_sm = WorkflowTaskMachine::new(self.next_started_event_id);
                let key = self.all_machines.insert(wf_task_sm.into());
                self.submachine_handle_event(key, event_dat)?;
                self.machines_by_event_id.insert(event_id, key);
            }
            Ok(EventType::WorkflowExecutionSignaled) => {
                if let Some(history_event::Attributes::WorkflowExecutionSignaledEventAttributes(
                    attrs,
                )) = event_dat.event.attributes
                {
                    self.drive_me
                        .send_job(workflow_activation::SignalWorkflow::from(attrs).into());
                } else {
                    // err
                }
            }
            Ok(EventType::WorkflowExecutionCancelRequested) => {
                if let Some(
                    history_event::Attributes::WorkflowExecutionCancelRequestedEventAttributes(
                        attrs,
                    ),
                ) = event_dat.event.attributes
                {
                    self.drive_me
                        .send_job(workflow_activation::CancelWorkflow::from(attrs).into());
                } else {
                    // err
                }
            }
            _ => {
                return Err(WFMachinesError::Fatal(format!(
                    "The event is not a non-stateful event, but we tried to handle it as one: {event_dat}"
                )));
            }
        }
        Ok(())
    }

    fn set_current_time(&mut self, time: SystemTime) -> SystemTime {
        if self.current_wf_time.is_none_or(|t| t < time) {
            self.current_wf_time = Some(time);
        }
        self.current_wf_time
            .expect("We have just ensured this is populated")
    }

    /// Wrapper for calling [TemporalStateMachine::handle_event] which appropriately takes action
    /// on the returned machine responses
    fn submachine_handle_event(&mut self, sm: MachineKey, event: HistEventData) -> Result<()> {
        let machine_responses = self.machine_mut(sm).handle_event(event)?;
        self.process_machine_responses(sm, machine_responses)?;
        Ok(())
    }
    /// Handle a single protocol message delivered in a workflow task.
    ///
    /// This function will attempt to apply the message to a corresponding state machine for the
    /// appropriate protocol type, creating it if it does not exist.
    ///
    /// On replay, protocol messages may be made up by looking ahead in history to see if there is
    /// already an event corresponding to the result of some protocol message which would have
    /// existed to create that result.
    #[instrument(skip(self))]
    fn handle_protocol_message(&mut self, message: IncomingProtocolMessage) -> Result<()> {
        static SEQIDERR: &str = "Update request messages must contain an event sequencing id! \
                                 This is a server bug.";

        match message.body {
            IncomingProtocolMessageBody::UpdateRequest(ur) => {
                let seq_id = if let SequencingId::EventId(eid) = message
                    .sequencing_id
                    .ok_or_else(|| WFMachinesError::Fatal(SEQIDERR.to_string()))?
                {
                    eid
                } else {
                    return Err(WFMachinesError::Fatal(SEQIDERR.to_string()));
                };
                let um = UpdateMachine::init(
                    message.id,
                    message.protocol_instance_id.clone(),
                    seq_id,
                    ur,
                    self.replaying,
                );
                let mk = self.add_new_protocol_machine(um.machine, message.protocol_instance_id);
                self.process_machine_responses(mk, vec![um.response])?;
            }
        }
        Ok(())
    }

    /// Transfer commands from `current_wf_task_commands` to `commands`, so they may be sent off
    /// to the server. While doing so, [TemporalStateMachine::handle_command] is called on the
    /// machine associated with the command.
    fn prepare_commands(&mut self) -> Result<()> {
        // It's possible we might prepare commands more than once before completing a WFT. (Because
        // of local activities, of course). Some commands might have since been cancelled that we
        // already prepared. Rip them out of the outgoing command list if so.
        self.commands.retain(|c| {
            !self
                .all_machines
                .get(c.machine)
                .expect("Machine must exist")
                .was_cancelled_before_sent_to_server()
        });

        while let Some(c) = self.current_wf_task_commands.pop_front() {
            if !self
                .machine(c.machine)
                .was_cancelled_before_sent_to_server()
            {
                match &c.command {
                    MachineAssociatedCommand::Real(cmd) => {
                        let machine_responses = self
                            .machine_mut(c.machine)
                            .handle_command(cmd.command_type())?;
                        self.process_machine_responses(c.machine, machine_responses)?;
                    }
                    MachineAssociatedCommand::FakeLocalActivityMarker(_) => {}
                }
                self.commands.push_back(c);
            }
        }
        debug!(commands = %self.commands.display(), "prepared commands");
        Ok(())
    }

    /// After a machine handles either an event or a command, it produces [MachineResponses] which
    /// this function uses to drive sending jobs to lang, triggering new workflow tasks, etc.
    fn process_machine_responses(
        &mut self,
        smk: MachineKey,
        machine_responses: Vec<MachineResponse>,
    ) -> Result<()> {
        let sm = self.machine(smk);
        if !machine_responses.is_empty() {
            trace!(responses = %machine_responses.display(), machine_name = %sm.name(),
                   "Machine produced responses");
        }
        for response in machine_responses {
            match response {
                MachineResponse::PushWFJob(a) => {
                    // We don't need to notify lang about jobs created by core-internal machines
                    if !self.machine_is_core_created.contains_key(smk) {
                        self.drive_me.send_job(a);
                    }
                }
                MachineResponse::TriggerWFTaskStarted {
                    task_started_event_id,
                    time,
                } => {
                    self.task_started(task_started_event_id, time)?;
                }
                MachineResponse::UpdateRunIdOnWorkflowReset { run_id: new_run_id } => {
                    self.drive_me.send_job(
                        workflow_activation_job::Variant::UpdateRandomSeed(UpdateRandomSeed {
                            randomness_seed: str_to_randomness_seed(&new_run_id),
                        })
                        .into(),
                    );
                }
                MachineResponse::IssueNewCommand(c) => {
                    self.current_wf_task_commands.push_back(CommandAndMachine {
                        command: MachineAssociatedCommand::Real(Box::new(c)),
                        machine: smk,
                    })
                }
                MachineResponse::IssueNewMessage(pm) => {
                    // Messages shouldn't be sent back when replaying. This is true for update,
                    // currently the only user of protocol messages. May eventually change.
                    if !self.replaying {
                        self.message_outbox.push_back(pm);
                    }
                }
                MachineResponse::NewCoreOriginatedCommand(attrs) => match attrs {
                    ProtoCmdAttrs::RequestCancelExternalWorkflowExecutionCommandAttributes(
                        attrs,
                    ) => {
                        let we = NamespacedWorkflowExecution {
                            namespace: attrs.namespace,
                            workflow_id: attrs.workflow_id,
                            run_id: attrs.run_id,
                        };
                        self.add_cmd_to_wf_task(
                            new_external_cancel(0, we, attrs.child_workflow_only, attrs.reason),
                            None,
                            CommandIdKind::CoreInternal,
                        );
                    }
                    ProtoCmdAttrs::UpsertWorkflowSearchAttributesCommandAttributes(attrs) => {
                        // We explicitly do not update the workflows current SAs here since
                        // core-generated upserts aren't meant to be modified or used within
                        // workflows by users (but rather, just for them to search with).
                        self.add_cmd_to_wf_task(
                            upsert_search_attrs_internal(attrs),
                            None,
                            CommandIdKind::NeverResolves,
                        );
                    }
                    c => {
                        return Err(WFMachinesError::Fatal(format!(
                            "A machine requested to create a new command of an unsupported type: {c:?}"
                        )));
                    }
                },
                MachineResponse::IssueFakeLocalActivityMarker(seq) => {
                    self.current_wf_task_commands.push_back(CommandAndMachine {
                        command: MachineAssociatedCommand::FakeLocalActivityMarker(seq),
                        machine: smk,
                    });
                }
                MachineResponse::QueueLocalActivity(act) => {
                    self.local_activity_data.enqueue(act);
                }
                MachineResponse::RequestCancelLocalActivity(seq) => {
                    // We might already know about the status from a pre-resolution. Apply it if so.
                    // We need to do this because otherwise we might need to perform additional
                    // activations during replay that didn't happen during execution, just like
                    // we sometimes pre-resolve activities when first requested.
                    if let Some(preres) = self.local_activity_data.take_preresolution(seq) {
                        if let Machines::LocalActivityMachine(lam) = self.machine_mut(smk) {
                            let more_responses = lam.try_resolve_with_dat(preres)?;
                            self.process_machine_responses(smk, more_responses)?;
                        } else {
                            panic!(
                                "A non local-activity machine returned a request cancel LA response"
                            );
                        }
                    }
                    // If it's in the request queue, just rip it out.
                    else if let Some(removed_act) =
                        self.local_activity_data.remove_from_queue(seq)
                    {
                        // We removed it. Notify the machine that the activity cancelled.
                        if let Machines::LocalActivityMachine(lam) = self.machine_mut(smk) {
                            let more_responses = lam.try_resolve(
                                LocalActivityExecutionResult::empty_cancel(),
                                Duration::from_secs(0),
                                removed_act.attempt,
                                None,
                                removed_act.original_schedule_time,
                            )?;
                            self.process_machine_responses(smk, more_responses)?;
                        } else {
                            panic!(
                                "A non local-activity machine returned a request cancel LA response"
                            );
                        }
                    } else {
                        // Finally, if we know about the LA at all, it's currently running, so
                        // queue the cancel request to be given to the LA manager.
                        self.local_activity_data.enqueue_cancel(ExecutingLAId {
                            run_id: self.run_id.clone(),
                            seq_num: seq,
                        });
                    }
                }
                MachineResponse::AbandonLocalActivity(seq) => {
                    self.local_activity_data.done_executing(seq);
                }
                MachineResponse::UpdateWFTime(t) => {
                    if let Some(t) = t {
                        self.set_current_time(t);
                    }
                }
            }
        }
        Ok(())
    }

    /// Called when a workflow task started event has triggered. Ensures we are tracking the ID
    /// of the current started event as well as workflow time properly.
    fn task_started(&mut self, task_started_event_id: i64, time: SystemTime) -> Result<()> {
        self.current_started_event_id = task_started_event_id;
        self.wft_start_time = Some(time);
        self.set_current_time(time);

        // Notify local activity machines that we started a non-replay WFT, which will allow any
        // which were waiting for a marker to instead decide to execute the LA since it clearly
        // will not be resolved via marker.
        if !self.replaying {
            let mut resps = vec![];
            for (k, mach) in self.all_machines.iter_mut() {
                if let Machines::LocalActivityMachine(lam) = mach {
                    resps.push((k, lam.encountered_non_replay_wft()?));
                }
            }
            for (mkey, resp_set) in resps {
                self.process_machine_responses(mkey, resp_set)?;
            }
        }
        Ok(())
    }

    /// Handles results of the workflow activation, delegating work to the appropriate state
    /// machine. Returns a list of workflow jobs that should be queued in the pending activation for
    /// the next poll. This list will be populated only if state machine produced lang activations
    /// as part of command processing. For example some types of activity cancellation need to
    /// immediately unblock lang side without having it to poll for an actual workflow task from the
    /// server.
    fn handle_driven_results(&mut self, results: Vec<WFCommand>) -> Result<()> {
        for cmd in results {
            match cmd.variant {
                WFCommandVariant::AddTimer(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(
                        new_timer(attrs),
                        cmd.metadata,
                        CommandID::Timer(seq).into(),
                    );
                }
                WFCommandVariant::UpsertSearchAttributes(attrs) => {
                    self.drive_me
                        .search_attributes_update(attrs.search_attributes.clone());
                    self.add_cmd_to_wf_task(
                        upsert_search_attrs(
                            attrs,
                            self.observed_internal_flags.clone(),
                            self.replaying,
                        ),
                        cmd.metadata,
                        CommandIdKind::NeverResolves,
                    );
                }
                WFCommandVariant::CancelTimer(attrs) => {
                    cancel_machine!(self, CommandID::Timer(attrs.seq), TimerMachine, cancel);
                }
                WFCommandVariant::AddActivity(attrs) => {
                    let seq = attrs.seq;
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_cmd_to_wf_task(
                        ActivityMachine::new_scheduled(
                            attrs,
                            self.observed_internal_flags.clone(),
                            use_compat,
                        ),
                        cmd.metadata,
                        CommandID::Activity(seq).into(),
                    );
                }
                WFCommandVariant::AddLocalActivity(attrs) => {
                    let seq = attrs.seq;
                    let attrs: ValidScheduleLA =
                        ValidScheduleLA::from_schedule_la(attrs).map_err(|e| {
                            WFMachinesError::Fatal(format!(
                                "Invalid schedule local activity request (seq {seq}): {e}"
                            ))
                        })?;
                    let (la, mach_resp) = new_local_activity(
                        attrs,
                        self.replaying,
                        self.local_activity_data.take_preresolution(seq),
                        self.current_wf_time,
                        self.observed_internal_flags.clone(),
                    )?;
                    let machkey = self.all_machines.insert(la.into());
                    self.id_to_machine
                        .insert(CommandID::LocalActivity(seq), machkey);
                    self.process_machine_responses(machkey, mach_resp)?;
                }
                WFCommandVariant::RequestCancelActivity(attrs) => {
                    cancel_machine!(
                        self,
                        CommandID::Activity(attrs.seq),
                        ActivityMachine,
                        cancel
                    );
                }
                WFCommandVariant::RequestCancelLocalActivity(attrs) => {
                    cancel_machine!(
                        self,
                        CommandID::LocalActivity(attrs.seq),
                        LocalActivityMachine,
                        cancel
                    );
                }
                WFCommandVariant::CompleteWorkflow(attrs) => {
                    self.add_terminal_command(complete_workflow(attrs), cmd.metadata);
                }
                WFCommandVariant::FailWorkflow(attrs) => {
                    self.add_terminal_command(fail_workflow(attrs), cmd.metadata);
                }
                WFCommandVariant::ContinueAsNew(attrs) => {
                    let attrs = self.augment_continue_as_new_with_current_values(attrs);
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_terminal_command(continue_as_new(attrs, use_compat), cmd.metadata);
                }
                WFCommandVariant::CancelWorkflow(attrs) => {
                    self.add_terminal_command(cancel_workflow(attrs), cmd.metadata);
                }
                WFCommandVariant::SetPatchMarker(attrs) => {
                    // Do not create commands for change IDs that we have already created commands
                    // for.
                    let encountered_entry = self.encountered_patch_markers.get(&attrs.patch_id);
                    if !matches!(encountered_entry,
                                 Some(ChangeInfo {created_command}) if *created_command)
                    {
                        let (patch_machine, other_cmds) = has_change(
                            attrs.patch_id.clone(),
                            self.replaying,
                            attrs.deprecated,
                            encountered_entry.is_some(),
                            self.encountered_patch_markers.keys().map(|s| s.as_str()),
                            self.observed_internal_flags.clone(),
                        )?;
                        let mkey = self.add_cmd_to_wf_task(
                            patch_machine,
                            cmd.metadata,
                            CommandIdKind::NeverResolves,
                        );
                        self.process_machine_responses(mkey, other_cmds)?;

                        if let Some(ci) = self.encountered_patch_markers.get_mut(&attrs.patch_id) {
                            ci.created_command = true;
                        } else {
                            self.encountered_patch_markers.insert(
                                attrs.patch_id,
                                ChangeInfo {
                                    created_command: true,
                                },
                            );
                        }
                    }
                }
                WFCommandVariant::AddChildWorkflow(attrs) => {
                    let seq = attrs.seq;
                    let use_compat = self.determine_use_compatible_flag(
                        attrs.versioning_intent(),
                        &attrs.task_queue,
                    );
                    self.add_cmd_to_wf_task(
                        ChildWorkflowMachine::new_scheduled(
                            attrs,
                            self.observed_internal_flags.clone(),
                            use_compat,
                        ),
                        cmd.metadata,
                        CommandID::ChildWorkflowStart(seq).into(),
                    );
                }
                WFCommandVariant::CancelChild(attrs) => {
                    cancel_machine!(
                        self,
                        CommandID::ChildWorkflowStart(attrs.child_workflow_seq),
                        ChildWorkflowMachine,
                        cancel,
                        attrs.reason
                    );
                }
                WFCommandVariant::RequestCancelExternalWorkflow(attrs) => {
                    let we = attrs.workflow_execution.ok_or_else(|| {
                        WFMachinesError::Fatal(
                            "Cancel external workflow command had no workflow_execution field"
                                .to_string(),
                        )
                    })?;
                    self.add_cmd_to_wf_task(
                        new_external_cancel(
                            attrs.seq,
                            we,
                            false,
                            format!(
                                "Cancel requested by workflow with run id {} with reason: {}",
                                self.run_id, attrs.reason
                            ),
                        ),
                        cmd.metadata,
                        CommandID::CancelExternal(attrs.seq).into(),
                    );
                }
                WFCommandVariant::SignalExternalWorkflow(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(
                        new_external_signal(attrs, &self.worker_config.namespace)?,
                        cmd.metadata,
                        CommandID::SignalExternal(seq).into(),
                    );
                }
                WFCommandVariant::CancelSignalWorkflow(attrs) => {
                    cancel_machine!(
                        self,
                        CommandID::SignalExternal(attrs.seq),
                        SignalExternalMachine,
                        cancel
                    );
                }
                WFCommandVariant::QueryResponse(_) => {
                    // Nothing to do here, queries are handled above the machine level
                    unimplemented!("Query responses should not make it down into the machines")
                }
                WFCommandVariant::ModifyWorkflowProperties(attrs) => {
                    self.add_cmd_to_wf_task(
                        modify_workflow_properties(attrs),
                        cmd.metadata,
                        CommandIdKind::NeverResolves,
                    );
                }
                WFCommandVariant::UpdateResponse(ur) => {
                    let m_key = self.get_machine_by_msg(&ur.protocol_instance_id)?;
                    let m = if let Machines::UpdateMachine(m) = self.machine_mut(m_key) {
                        m
                    } else {
                        return Err(WFMachinesError::Nondeterminism(format!(
                            "Tried to handle an update response for \
                             update with instance id {} but it was not found!",
                            &ur.protocol_instance_id
                        )));
                    };
                    let resps = m.handle_response(ur)?;
                    self.process_machine_responses(m_key, resps)?;
                }
                WFCommandVariant::ScheduleNexusOperation(attrs) => {
                    let seq = attrs.seq;
                    self.add_cmd_to_wf_task(
                        NexusOperationMachine::new_scheduled(attrs),
                        cmd.metadata,
                        CommandID::NexusOperation(seq).into(),
                    );
                }
                WFCommandVariant::RequestCancelNexusOperation(attrs) => {
                    cancel_machine!(
                        self,
                        CommandID::NexusOperation(attrs.seq),
                        NexusOperationMachine,
                        cancel
                    );
                }
                WFCommandVariant::NoCommandsFromLang => (),
            }
        }
        Ok(())
    }

    fn get_machine_key(&self, id: CommandID) -> Result<MachineKey> {
        Ok(*self.id_to_machine.get(&id).ok_or_else(|| {
            WFMachinesError::Nondeterminism(format!("Missing associated machine for {id:?}"))
        })?)
    }

    fn get_machine_by_msg(&self, protocol_instance_id: &str) -> Result<MachineKey> {
        Ok(*self
            .machines_by_protocol_instance_id
            .get(protocol_instance_id)
            .ok_or_else(|| {
                WFMachinesError::Fatal(format!(
                    "Missing associated machine for protocol message {protocol_instance_id}"
                ))
            })?)
    }

    fn machine(&self, m: MachineKey) -> &Machines {
        self.all_machines.get(m).expect("Machine must exist")
    }

    fn machine_mut(&mut self, m: MachineKey) -> &mut Machines {
        self.all_machines.get_mut(m).expect("Machine must exist")
    }

    fn add_terminal_command(
        &mut self,
        machine: NewMachineWithCommand,
        metadata: Option<UserMetadata>,
    ) {
        let cwfm = self.add_new_command_machine(machine, metadata);
        self.workflow_end_time = Some(SystemTime::now());
        self.current_wf_task_commands.push_back(cwfm);
        // Wipe out any pending / executing local activity data since we're about to terminate
        // and there's nothing to be done with them.
        self.local_activity_data.indicate_terminating();
    }

    /// Add a new command/machines for that command to the current workflow task
    fn add_cmd_to_wf_task(
        &mut self,
        machine: NewMachineWithCommand,
        metadata: Option<UserMetadata>,
        id: CommandIdKind,
    ) -> MachineKey {
        let mach = self.add_new_command_machine(machine, metadata);
        let key = mach.machine;
        if let CommandIdKind::LangIssued(id) = id {
            self.id_to_machine.insert(id, key);
        }
        if matches!(id, CommandIdKind::CoreInternal) {
            self.machine_is_core_created.insert(key, ());
        }
        self.current_wf_task_commands.push_back(mach);
        key
    }

    fn add_new_command_machine(
        &mut self,
        machine: NewMachineWithCommand,
        metadata: Option<UserMetadata>,
    ) -> CommandAndMachine {
        let k = self.all_machines.insert(machine.machine);
        let cmd = ProtoCommand {
            command_type: machine.command.as_type() as i32,
            attributes: Some(machine.command),
            user_metadata: metadata,
        };
        CommandAndMachine {
            command: MachineAssociatedCommand::Real(Box::new(cmd)),
            machine: k,
        }
    }

    fn add_new_protocol_machine(&mut self, machine: Machines, instance_id: String) -> MachineKey {
        let k = self.all_machines.insert(machine);
        self.machines_by_protocol_instance_id.insert(instance_id, k);
        k
    }

    fn augment_continue_as_new_with_current_values(
        &self,
        mut attrs: ContinueAsNewWorkflowExecution,
    ) -> ContinueAsNewWorkflowExecution {
        if let Some(started_info) = self.drive_me.get_started_info() {
            if attrs.memo.is_empty() {
                attrs.memo = started_info
                    .memo
                    .clone()
                    .map(Into::into)
                    .unwrap_or_default();
            }
            if attrs.retry_policy.is_none() {
                attrs.retry_policy.clone_from(&started_info.retry_policy);
            }
        }
        if attrs.search_attributes.is_empty() {
            attrs.search_attributes = self.drive_me.get_current_search_attributes();
        }
        attrs
    }

    /// Given a user's versioning intent for a command and that command's target task queue,
    /// returns whether or not the command should set the flag for attempting to stick within the
    /// compatible version set
    fn determine_use_compatible_flag(&self, intent: VersioningIntent, target_tq: &str) -> bool {
        match intent {
            VersioningIntent::Compatible => true,
            VersioningIntent::Default => false,
            VersioningIntent::Unspecified => {
                // If the target TQ is empty, that means use same TQ.
                // When TQs match, use compat by default
                target_tq.is_empty() || target_tq == self.worker_config.task_queue
            }
        }
    }
}

/// Contains everything workflow machine internals need to bubble up when we're getting ready to
/// respond with a WFT completion. Allows for lazy mutation of the machine, since mutation is not
/// desired unless we are actually going to respond to the WFT, which may not always happen.
pub(crate) struct MachinesWFTResponseContent<'a> {
    me: &'a mut WorkflowMachines,
    pub(crate) replaying: bool,
    pub(crate) has_pending_jobs: bool,
    pub(crate) have_seen_terminal_event: bool,
    pub(crate) have_pending_la_resolutions: bool,
}

impl MachinesWFTResponseContent<'_> {
    pub(crate) fn commands(&self) -> Peekable<impl Iterator<Item = ProtoCommand> + '_> {
        self.me.get_commands().peekable()
    }

    pub(crate) fn has_messages(&self) -> bool {
        !self.me.message_outbox.is_empty()
    }

    pub(crate) fn messages(&mut self) -> Vec<ProtocolMessage> {
        self.me.message_outbox.drain(..).collect()
    }

    pub(crate) fn metadata_for_complete(&mut self) -> WorkflowTaskCompletedMetadata {
        self.me.get_metadata_for_wft_complete()
    }
}

fn str_to_randomness_seed(run_id: &str) -> u64 {
    // This was originally `DefaultHasher` but that is potentially unstable across Rust releases.
    // This must forever be `SipHasher13` now or we risk breaking history compat.
    let mut s = SipHasher13::new();
    run_id.hash(&mut s);
    s.finish()
}

#[must_use]
enum EventHandlingOutcome {
    SkipEvent { skip_next_event: bool },
    SkipCommand,
    Normal,
}

/// Special handling for patch markers, when handling command events as in
/// [WorkflowMachines::handle_command_event]
fn patch_marker_handling(
    event: &HistoryEvent,
    mach: Option<&Machines>,
    next_event: Option<&HistoryEvent>,
) -> Result<EventHandlingOutcome> {
    let patch_machine = match mach {
        Some(Machines::PatchMachine(pm)) => Some(pm),
        _ => None,
    };
    let patch_details = event.get_patch_marker_details();
    fn skip_one_or_two_events(next_event: Option<&HistoryEvent>) -> Result<EventHandlingOutcome> {
        // Also ignore the subsequent upsert event if present
        let mut skip_next_event = false;
        if let Some(history_event::Attributes::UpsertWorkflowSearchAttributesEventAttributes(atts)) =
            next_event.and_then(|ne| ne.attributes.as_ref())
            && let Some(ref sa) = atts.search_attributes
        {
            skip_next_event = sa.indexed_fields.contains_key(VERSION_SEARCH_ATTR_KEY);
        }

        Ok(EventHandlingOutcome::SkipEvent { skip_next_event })
    }

    if let Some((patch_name, deprecated)) = patch_details {
        if let Some(pm) = patch_machine {
            // If the next machine *is* a patch machine, but this marker is deprecated, it may
            // either apply to this machine (the `deprecate_patch` call is still in workflow code) -
            // or it could be another `patched` or `deprecate_patch` call for a *different* patch,
            // which we should also permit. In the latter case, we should skip this event.
            if !pm.matches_patch(&patch_name) && deprecated {
                skip_one_or_two_events(next_event)
            } else {
                Ok(EventHandlingOutcome::Normal)
            }
        } else {
            // Version markers can be skipped in the event they are deprecated. We can simply
            // ignore this event, as deprecated change markers are allowed without matching changed
            // calls.
            if deprecated {
                debug!("Deprecated patch marker tried against non-patch machine, skipping.");
                skip_one_or_two_events(next_event)
            } else {
                Err(WFMachinesError::Nondeterminism(format!(
                    "Non-deprecated patch marker encountered for change {patch_name}, but there is \
                     no corresponding change command!"
                )))
            }
        }
    } else if patch_machine.is_some() {
        debug!("Skipping non-matching event against patch machine");
        Ok(EventHandlingOutcome::SkipCommand)
    } else {
        // Not a patch machine or a patch event
        Ok(EventHandlingOutcome::Normal)
    }
}

#[derive(derive_more::From)]
enum CommandIdKind {
    /// A normal command, requested by lang
    LangIssued(CommandID),
    /// A command created internally
    CoreInternal,
    /// A command which is fire-and-forget (ex: Upsert search attribs)
    NeverResolves,
}
