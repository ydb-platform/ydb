#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ApplicationFailure, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};

#[workflow]
pub struct UpdatableTimerWorkflow {
    deadline_ms: u64,
    deadline_version: u64,
}

#[workflow_methods]
impl UpdatableTimerWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, initial_deadline_ms: u64) -> Self {
        Self {
            deadline_ms: initial_deadline_ms,
            deadline_version: 0,
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        loop {
            let current_version = ctx.state(|s| s.deadline_version);
            let deadline_ms = ctx.state(|s| s.deadline_ms);

            let now_ms = ctx
                .workflow_time()
                .ok_or_else(|| ApplicationFailure::new("Did not find workflow time"))?
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if deadline_ms <= now_ms {
                return Ok(format!("Timer fired at deadline {deadline_ms}"));
            }

            let remaining = Duration::from_millis(deadline_ms - now_ms);

            temporalio_sdk::workflows::select! {
                _ = ctx.timer(remaining) => {
                    let final_deadline = ctx.state(|s| s.deadline_ms);
                    return Ok(format!("Timer fired at deadline {final_deadline}"));
                }
                _ = ctx.wait_condition(|s: &Self| s.deadline_version != current_version) => {
                }
            }
        }
    }

    #[signal]
    pub fn update_deadline(&mut self, _ctx: &mut SyncWorkflowContext<Self>, new_deadline_ms: u64) {
        self.deadline_ms = new_deadline_ms;
        self.deadline_version += 1;
    }

    #[query]
    pub fn get_deadline(&self, _ctx: &WorkflowContextView) -> u64 {
        self.deadline_ms
    }
}
