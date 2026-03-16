use crate::{
    telemetry::VecDisplayer,
    worker::workflow::{OutgoingJob, WFCommand, WFCommandVariant, WorkflowStartedInfo},
};
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    sync::mpsc::{self, Receiver, Sender},
};
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::{WorkflowActivationJob, start_workflow_from_attribs},
    temporal::api::{common::v1::Payload, history::v1::WorkflowExecutionStartedEventAttributes},
    utilities::TryIntoOrNone,
};

/// Represents a connection to a lang side workflow that can have activations fed into it and
/// command responses pulled out.
pub(crate) struct DrivenWorkflow {
    started_attrs: Option<WorkflowStartedInfo>,
    search_attribute_modifications: HashMap<String, Payload>,
    incoming_commands: Receiver<Vec<WFCommand>>,
    /// Outgoing activation jobs that need to be sent to the lang sdk
    outgoing_wf_activation_jobs: Vec<OutgoingJob>,
}

impl DrivenWorkflow {
    pub(super) fn new() -> (Self, Sender<Vec<WFCommand>>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                started_attrs: None,
                search_attribute_modifications: Default::default(),
                incoming_commands: rx,
                outgoing_wf_activation_jobs: vec![],
            },
            tx,
        )
    }
    /// Start the workflow
    pub(super) fn start(
        &mut self,
        workflow_id: String,
        randomness_seed: u64,
        start_time: Timestamp,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) {
        debug!(run_id = %attribs.original_execution_run_id, "Driven WF start");
        let started_info = WorkflowStartedInfo {
            workflow_task_timeout: attribs.workflow_task_timeout.try_into_or_none(),
            memo: attribs.memo.clone(),
            search_attrs: attribs.search_attributes.clone(),
            retry_policy: attribs.retry_policy.clone(),
        };
        self.send_job(
            start_workflow_from_attribs(attribs, workflow_id, randomness_seed, start_time).into(),
        );
        self.started_attrs = Some(started_info);
    }

    /// Return the attributes from the workflow execution started event if this workflow has started
    pub(super) fn get_started_info(&self) -> Option<&WorkflowStartedInfo> {
        self.started_attrs.as_ref()
    }

    /// Enqueue a new job to be sent to the driven workflow
    pub(super) fn send_job(&mut self, job: OutgoingJob) {
        self.outgoing_wf_activation_jobs.push(job);
    }

    /// Observe pending jobs
    pub(super) fn peek_pending_jobs(&self) -> &[OutgoingJob] {
        self.outgoing_wf_activation_jobs.as_slice()
    }

    /// Drain all pending jobs, so that they may be sent to the driven workflow
    pub(super) fn drain_jobs(&mut self) -> Vec<WorkflowActivationJob> {
        self.outgoing_wf_activation_jobs
            .drain(..)
            .map(Into::into)
            .collect()
    }

    /// Obtain any output from the workflow's recent execution(s). Because the lang sdk is
    /// responsible for calling workflow code as a result of receiving tasks from
    /// [crate::Core::poll_task], we cannot directly iterate it here. Commands are simply pulled
    /// from a buffer that the language side sinks into when it calls [crate::Core::complete_task]
    pub(super) fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        let in_cmds = self.incoming_commands.try_recv();
        let in_cmds = in_cmds.unwrap_or_else(|_| {
            vec![WFCommand {
                variant: WFCommandVariant::NoCommandsFromLang,
                metadata: None,
            }]
        });
        debug!(in_cmds = %in_cmds.display(), "wf bridge iteration fetch");
        in_cmds
    }

    /// Lang sent us an SA upsert command - use it to update our current view of search attributes.
    pub(crate) fn search_attributes_update(&mut self, update: HashMap<String, Payload>) {
        self.search_attribute_modifications.extend(update);
    }

    /// Return a view of the "current" state of search attributes. IE: The initial attributes
    /// plus any changes during the lifetime of the workflow.
    pub(crate) fn get_current_search_attributes(&self) -> HashMap<String, Payload> {
        let mut retme = self
            .started_attrs
            .as_ref()
            .and_then(|si| si.search_attrs.as_ref().map(|sa| sa.indexed_fields.clone()))
            .unwrap_or_default();
        retme.extend(
            self.search_attribute_modifications
                .iter()
                .map(|(a, b)| (a.clone(), b.clone())),
        );
        retme
    }
}
