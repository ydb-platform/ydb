#![allow(unreachable_pub)]

use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GreetingRequest {
    pub name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GreetingResponse {
    pub message: String,
}

#[workflow]
#[derive(Default)]
pub struct ActivityInterceptorWorkflow;

#[workflow_methods]
impl ActivityInterceptorWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greet = ctx
            .execute_activity(
                GreetingActivities::greet,
                GreetingRequest { name: name.clone() },
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;
        let shout = ctx
            .execute_activity(
                GreetingActivities::shout,
                GreetingRequest { name },
                ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
            )
            .await?;
        Ok(format!("{} {}", greet.message, shout.message))
    }
}

pub struct GreetingActivities;

#[activities]
impl GreetingActivities {
    #[activity]
    pub async fn greet(
        _ctx: ActivityContext,
        request: GreetingRequest,
    ) -> Result<GreetingResponse, ActivityError> {
        Ok(GreetingResponse {
            message: format!("Hello, {}!", request.name),
        })
    }

    #[activity]
    pub async fn shout(
        _ctx: ActivityContext,
        request: GreetingRequest,
    ) -> Result<GreetingResponse, ActivityError> {
        Ok(GreetingResponse {
            message: format!("HELLO, {}!", request.name.to_uppercase()),
        })
    }
}
