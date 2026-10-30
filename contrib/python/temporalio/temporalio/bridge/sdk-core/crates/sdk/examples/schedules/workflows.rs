#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct ScheduledActivities;

#[activities]
impl ScheduledActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
#[derive(Default)]
pub struct ScheduledWorkflow;

#[workflow_methods]
impl ScheduledWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx
            .execute_activity(
                ScheduledActivities::greet,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;
        Ok(greeting)
    }
}
