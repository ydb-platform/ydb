use super::super::local_activity_state_machine::ResolveDat;
use crate::{
    protosext::{CompleteLocalActivityData, ValidScheduleLA},
    worker::{ExecutingLAId, LocalActRequest, NewLocalAct},
};
use std::{
    collections::{HashMap, HashSet},
    time::SystemTime,
};
use temporal_sdk_core_protos::temporal::api::common::v1::WorkflowExecution;

#[derive(Default)]
pub(super) struct LocalActivityData {
    /// Queued local activity requests which need to be executed
    new_requests: Vec<ValidScheduleLA>,
    /// Queued cancels that need to be dispatched
    cancel_requests: Vec<ExecutingLAId>,
    /// Seq #s of local activities which we have sent to be executed but have not yet resolved
    executing: HashSet<u32>,
    /// Maps local activity sequence numbers to their resolutions as found when looking ahead at
    /// next WFT
    preresolutions: HashMap<u32, ResolveDat>,
    /// Set true if the workflow is terminating
    am_terminating: bool,
}

impl LocalActivityData {
    pub(super) fn enqueue(&mut self, act: ValidScheduleLA) {
        self.new_requests.push(act);
    }

    pub(super) fn enqueue_cancel(&mut self, cancel: ExecutingLAId) {
        self.cancel_requests.push(cancel);
    }

    pub(super) fn done_executing(&mut self, seq: u32) {
        // This seems nonsense, but can happen during abandonment
        self.new_requests.retain(|req| req.seq != seq);
        self.executing.remove(&seq);
    }

    /// Drain all requests to execute or cancel LAs. Additional info is passed in to be able to
    /// augment the data this struct has to form complete request data.
    pub(super) fn take_all_reqs(
        &mut self,
        wf_type: &str,
        wf_id: &str,
        run_id: &str,
    ) -> Vec<LocalActRequest> {
        if self.am_terminating {
            return vec![LocalActRequest::CancelAllInRun(run_id.to_string())];
        }

        self.cancel_requests
            .drain(..)
            .map(LocalActRequest::Cancel)
            .chain(self.new_requests.drain(..).map(|sa| {
                self.executing.insert(sa.seq);
                LocalActRequest::New(NewLocalAct {
                    schedule_time: SystemTime::now(),
                    schedule_cmd: sa,
                    workflow_type: wf_type.to_string(),
                    workflow_exec_info: WorkflowExecution {
                        workflow_id: wf_id.to_string(),
                        run_id: run_id.to_string(),
                    },
                })
            }))
            .collect()
    }

    /// Returns all outstanding local activities, whether executing or requested and in the queue
    pub(super) fn outstanding_la_count(&self) -> usize {
        if self.am_terminating {
            return 0;
        }
        self.executing.len() + self.new_requests.len()
    }

    pub(super) fn insert_peeked_marker(&mut self, dat: CompleteLocalActivityData) {
        self.preresolutions.insert(dat.marker_dat.seq, dat.into());
    }

    pub(super) fn take_preresolution(&mut self, seq: u32) -> Option<ResolveDat> {
        self.preresolutions.remove(&seq)
    }

    pub(super) fn remove_from_queue(&mut self, seq: u32) -> Option<ValidScheduleLA> {
        self.new_requests
            .iter()
            .position(|req| req.seq == seq)
            .map(|i| self.new_requests.remove(i))
    }

    /// Store that this workflow is terminating, and thus no new LA requests need be processed,
    /// and any executing LAs should not prevent us from shutting down.
    pub(super) fn indicate_terminating(&mut self) {
        self.am_terminating = true;
    }
}
