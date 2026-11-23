#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct HeartbeatingActivities;

#[activities]
impl HeartbeatingActivities {
    /// Simulates a long-running activity that heartbeats its progress.
    /// On retry, it resumes from the last heartbeat checkpoint.
    #[activity]
    pub async fn long_running_activity(
        ctx: ActivityContext,
        total_steps: u32,
    ) -> Result<String, ActivityError> {
        let start_step: u32 = ctx.heartbeat_details().deserialize()?.unwrap_or(0);

        for step in start_step..total_steps {
            if ctx.is_cancelled() {
                return Err(ActivityError::cancelled());
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            ctx.record_heartbeat(step).await?;
        }

        Ok(format!("Completed {total_steps} steps"))
    }
}

#[workflow]
#[derive(Default)]
pub struct HeartbeatingWorkflow;

#[workflow_methods]
impl HeartbeatingWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, total_steps: u32) -> WorkflowResult<String> {
        let result = ctx
            .execute_activity(
                HeartbeatingActivities::long_running_activity,
                total_steps,
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .heartbeat_timeout(Duration::from_secs(5))
                    .build(),
            )
            .await?;
        Ok(result)
    }
}
