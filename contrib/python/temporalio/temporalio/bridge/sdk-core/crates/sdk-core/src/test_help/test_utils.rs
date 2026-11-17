//! Test utilities for creating workflow commands and histories
//! Only available when the test-utilities feature is enabled

use crate::replay::DEFAULT_ACTIVITY_TYPE;
use std::time::Duration;
use temporalio_common::protos::{
    coresdk::workflow_commands::{
        ActivityCancellationType, QueryResult, QuerySuccess, ScheduleActivity,
        ScheduleLocalActivity, StartTimer, workflow_command,
    },
    temporal::api::common::v1::Payload,
};

/// Create a start timer command for use in tests
pub fn start_timer_cmd(seq: u32, duration: Duration) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(duration.try_into().expect("duration fits")),
    }
    .into()
}

/// Create a schedule activity command for use in tests
pub fn schedule_activity_cmd(
    seq: u32,
    task_q: &str,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    activity_timeout: Duration,
    heartbeat_timeout: Duration,
) -> workflow_command::Variant {
    ScheduleActivity {
        seq,
        activity_id: activity_id.to_string(),
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        task_queue: task_q.to_owned(),
        schedule_to_start_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        start_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        schedule_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        heartbeat_timeout: Some(heartbeat_timeout.try_into().expect("duration fits")),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
    .into()
}

/// Create a schedule local activity command for use in tests
pub fn schedule_local_activity_cmd(
    seq: u32,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    activity_timeout: Duration,
) -> workflow_command::Variant {
    ScheduleLocalActivity {
        seq,
        activity_id: activity_id.to_string(),
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        schedule_to_start_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        start_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        schedule_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
    .into()
}

/// Create a successful query response command for use in tests
pub fn query_ok(id: impl Into<String>, response: impl Into<Payload>) -> workflow_command::Variant {
    QueryResult {
        query_id: id.into(),
        variant: Some(
            QuerySuccess {
                response: Some(response.into()),
            }
            .into(),
        ),
    }
    .into()
}
