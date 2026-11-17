#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct PollingActivities;

#[activities]
impl PollingActivities {
    #[activity]
    pub async fn check_condition(
        _ctx: ActivityContext,
        attempt: u32,
    ) -> Result<bool, ActivityError> {
        Ok(attempt >= 3)
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions::start_to_close_timeout(Duration::from_secs(10))
}

#[workflow]
#[derive(Default)]
pub struct PollingWorkflow;

#[workflow_methods]
impl PollingWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, max_attempts: u32) -> WorkflowResult<String> {
        for attempt in 1..=max_attempts {
            let is_ready = ctx
                .execute_activity(PollingActivities::check_condition, attempt, activity_opts())
                .await?;

            if is_ready {
                return Ok(format!("Condition met on attempt {attempt}"));
            }

            if attempt < max_attempts {
                ctx.timer(Duration::from_secs(1)).await;
            }
        }

        Ok(format!("Condition not met after {max_attempts} attempts"))
    }
}
