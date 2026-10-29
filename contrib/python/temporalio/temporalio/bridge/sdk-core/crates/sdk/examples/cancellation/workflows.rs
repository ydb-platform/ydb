#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, WorkflowCancellationToken, WorkflowContext,
    WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct CancellationActivities;

#[activities]
impl CancellationActivities {
    #[activity]
    pub async fn long_running_activity(
        ctx: ActivityContext,
        _input: (),
    ) -> Result<String, ActivityError> {
        loop {
            if ctx.is_cancelled() {
                return Err(ActivityError::cancelled());
            }
            ctx.record_heartbeat(()).await?;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    #[activity]
    pub async fn cleanup(_ctx: ActivityContext, _input: ()) -> Result<String, ActivityError> {
        Ok("cleanup done".to_string())
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
        .heartbeat_timeout(Duration::from_secs(5))
        .build()
}

#[workflow]
#[derive(Default)]
pub struct CancellationWorkflow;

#[workflow_methods]
impl CancellationWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<String> {
        let result = ctx
            .execute_activity(
                CancellationActivities::long_running_activity,
                (),
                activity_opts(),
            )
            .await;
        match result {
            Ok(value) => Ok(value),
            Err(ActivityExecutionError::Cancelled(_)) => {
                let reason = ctx.cancellation_token().reason().unwrap_or_default();
                let cleanup_result = ctx
                    .execute_activity(
                        CancellationActivities::cleanup,
                        (),
                        ActivityOptions::with_start_to_close_timeout(Duration::from_secs(10))
                            // Use a cancellation token disconected from the workflow cancellation to
                            // ensure cleanup activity is run.
                            .cancellation_token(WorkflowCancellationToken::new())
                            .build(),
                    )
                    .await?;

                Ok(format!("Cancelled (reason={reason}), {cleanup_result}"))
            }
            Err(err) => Err(err.into()),
        }
    }
}
