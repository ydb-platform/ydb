#![allow(unreachable_pub)]
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct MessagePassingWorkflow {
    counter: i32,
}

#[workflow_methods]
impl MessagePassingWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, target_value: i32) -> WorkflowResult<i32> {
        ctx.wait_condition(|s| s.counter >= target_value).await?;
        Ok(ctx.state(|s| s.counter))
    }

    #[signal]
    pub fn increment(&mut self, _ctx: &mut SyncWorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    #[query]
    pub fn get_counter(&self, _ctx: &WorkflowContextView) -> i32 {
        self.counter
    }

    #[update_validator(set_counter)]
    fn validate_set_counter(
        &self,
        _ctx: &WorkflowContextView,
        value: &i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *value < 0 {
            Err("Counter value must be non-negative".into())
        } else {
            Ok(())
        }
    }

    #[update]
    pub fn set_counter(&mut self, _ctx: &mut SyncWorkflowContext<Self>, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
    }
}
