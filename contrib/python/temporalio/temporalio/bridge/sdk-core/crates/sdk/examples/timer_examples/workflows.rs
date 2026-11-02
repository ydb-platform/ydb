#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, TimerResult, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub struct TimerActivities;

#[activities]
impl TimerActivities {
    #[activity]
    pub async fn slow_activity(
        _ctx: ActivityContext,
        delay_ms: u64,
    ) -> Result<String, ActivityError> {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        Ok("activity completed".to_string())
    }
}

#[workflow]
#[derive(Default)]
pub struct TimerWorkflow;

#[workflow_methods]
impl TimerWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        ctx.timer(Duration::from_secs(1)).await;

        let winner = temporalio_sdk::workflows::select! {
            _ = ctx.timer(Duration::from_secs(10)) => "timer",
            result = ctx.execute_activity(
                TimerActivities::slow_activity,
                100u64,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(30)),
            ) => {
                result?;
                "activity"
            }
        };

        let timer_fut = ctx.timer(Duration::from_secs(600));
        timer_fut.cancel();
        let timer_result = timer_fut.await;
        let was_cancelled = timer_result == TimerResult::Cancelled;

        Ok(format!(
            "race_winner={winner}, timer_cancelled={was_cancelled}"
        ))
    }
}
