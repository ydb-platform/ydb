use crate::prost_dur;
use std::time::Duration;
use temporal_sdk::{ActivityOptions, LocalActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::{coresdk::AsJsonPayloadExt, temporal::api::common::v1::RetryPolicy};

pub async fn la_problem_workflow(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: "delay".to_string(),
        input: "hi".as_json_payload().expect("serializes fine"),
        retry_policy: RetryPolicy {
            initial_interval: Some(prost_dur!(from_micros(15))),
            backoff_coefficient: 1_000.,
            maximum_interval: Some(prost_dur!(from_millis(1500))),
            maximum_attempts: 4,
            non_retryable_error_types: vec![],
        },
        timer_backoff_threshold: Some(Duration::from_secs(1)),
        ..Default::default()
    })
    .await;
    ctx.activity(ActivityOptions {
        activity_type: "delay".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(20)),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    Ok(().into())
}
