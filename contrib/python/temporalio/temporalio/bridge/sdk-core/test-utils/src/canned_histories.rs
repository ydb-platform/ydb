use prost::Message;
use rand::RngCore;
use std::{fs::File, io::Write, path::PathBuf};
use temporal_sdk_core::replay::TestHistoryBuilder;
use temporal_sdk_core_protos::{
    coresdk::common::NamespacedWorkflowExecution,
    temporal::api::{
        common::v1::{Payload, WorkflowExecution},
        enums::v1::{EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::*,
    },
};

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_timer(timer_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, timer_id.to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

pub fn single_timer_wf_completes(timer_id: &str) -> TestHistoryBuilder {
    let mut t = single_timer(timer_id);
    t.add_workflow_task_completed();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED (cancel)
///  6: EVENT_TYPE_TIMER_STARTED (wait)
///  7: EVENT_TYPE_TIMER_FIRED (wait)
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_TIMER_CANCELED (cancel)
/// 12: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_timer(wait_timer_id: &str, cancel_timer_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let cancel_timer_started_id = t.add_by_type(EventType::TimerStarted);
    let wait_timer_started_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(wait_timer_started_id, wait_timer_id.to_string());
    // 8
    t.add_full_wf_task();
    // 11
    t.add(history_event::Attributes::TimerCanceledEventAttributes(
        TimerCanceledEventAttributes {
            started_event_id: cancel_timer_started_id,
            timer_id: cancel_timer_id.to_string(),
            ..Default::default()
        },
    ));
    // 12
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_TIMER_STARTED
/// 6: EVENT_TYPE_TIMER_STARTED
/// 7: EVENT_TYPE_TIMER_FIRED
/// 8: EVENT_TYPE_TIMER_FIRED
/// 9: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 10: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn parallel_timer(timer1: &str, timer2: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    let timer_2_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, timer1.to_string());
    t.add_timer_fired(timer_2_started_event_id, timer2.to_string());
    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn workflow_fails_with_reset_after_timer(
    timer_id: &str,
    original_run_id: &str,
) -> TestHistoryBuilder {
    let mut t = single_timer(timer_id);
    t.add_workflow_task_failed_new_id(WorkflowTaskFailedCause::ResetWorkflow, original_run_id);

    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn workflow_fails_with_failure_after_timer(timer_id: &str) -> TestHistoryBuilder {
    let mut t = single_timer(timer_id);
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure {
            message: "boom".to_string(),
            ..Default::default()
        },
    );

    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 10: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 12: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 13: EVENT_TYPE_TIMER_STARTED
/// 14: EVENT_TYPE_TIMER_FIRED
/// 15: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 16: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 17: EVENT_TYPE_WORKFLOW_TASK_FAILED
/// 18: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 19: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn workflow_fails_with_failure_two_different_points(
    timer_1: &str,
    timer_2: &str,
) -> TestHistoryBuilder {
    let mut t = single_timer(timer_1);
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure {
            message: "boom 1".to_string(),
            ..Default::default()
        },
    );
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, timer_2.to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure,
        Failure {
            message: "boom 2".to_string(),
            ..Default::default()
        },
    );
    t.add_workflow_task_scheduled_and_started();

    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_ACTIVITY_TASK_STARTED
///  7: EVENT_TYPE_ACTIVITY_TASK_COMPLETED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_activity(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, Default::default());
    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_MARKER_RECORDED
///  6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  7: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_local_activity(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, activity_id, Default::default());
    t.add_workflow_task_scheduled_and_started();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_ACTIVITY_TASK_STARTED
///  7: EVENT_TYPE_ACTIVITY_TASK_FAILED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn single_failed_activity(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add(
        history_event::Attributes::ActivityTaskFailedEventAttributes(
            ActivityTaskFailedEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_workflow_task_scheduled_and_started();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_scheduled_activity(activity_id: &str, signal_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn scheduled_activity_timeout(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add(
        history_event::Attributes::ActivityTaskTimedOutEventAttributes(
            ActivityTaskTimedOutEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
/// 12: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 13: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 14: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 15: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn scheduled_cancelled_activity_timeout(
    activity_id: &str,
    signal_id: &str,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add(
        history_event::Attributes::ActivityTaskTimedOutEventAttributes(
            ActivityTaskTimedOutEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 7: EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
/// 8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn started_activity_timeout(activity_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let started_event_id = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add(
        history_event::Attributes::ActivityTaskTimedOutEventAttributes(
            ActivityTaskTimedOutEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_scheduled_activity_abandon(activity_id: &str, signal_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_activity_task_scheduled(activity_id);
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 7: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_started_activity_abandon(activity_id: &str, signal_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 12: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 13: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 14: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 15: EVENT_TYPE_ACTIVITY_TASK_CANCELED
/// 16: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 17: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 18: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 19: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_scheduled_activity_with_signal_and_activity_task_cancel(
    activity_id: &str,
    signal_id: &str,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCanceledEventAttributes(
            ActivityTaskCanceledEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 7: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_ACTIVITY_TASK_CANCELED
/// 17: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 18: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 19: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 20: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_started_activity_with_signal_and_activity_task_cancel(
    activity_id: &str,
    signal_id: &str,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCanceledEventAttributes(
            ActivityTaskCanceledEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_ACTIVITY_TASK_CANCELED
/// 12: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 13: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 14: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 15: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_scheduled_activity_with_activity_task_cancel(
    activity_id: &str,
    signal_id: &str,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add(
        history_event::Attributes::ActivityTaskCanceledEventAttributes(
            ActivityTaskCanceledEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 7: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 12: EVENT_TYPE_ACTIVITY_TASK_CANCELED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_started_activity_with_activity_task_cancel(
    activity_id: &str,
    signal_id: &str,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add_we_signaled(
        signal_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add(
        history_event::Attributes::ActivityTaskCanceledEventAttributes(
            ActivityTaskCanceledEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

/// First signal's payload is "hello " and second is "world" (no metadata for either)
/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn two_signals(sig_1_id: &str, sig_2_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled(
        sig_1_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_we_signaled(
        sig_2_id,
        vec![Payload {
            metadata: Default::default(),
            data: b"world".to_vec(),
        }],
    );
    t.add_workflow_task_scheduled_and_started();
    t
}

/// Can produce long histories that look like below. `num_tasks` must be > 1.
///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// --- Repeat num_tasks - 1 (the above is counted) times ---
///  x: EVENT_TYPE_TIMER_STARTED
///  x: EVENT_TYPE_TIMER_FIRED
///  x: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  x: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  x: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// --- End repeat ---
/// 4 + (num tasks - 1) * 4 + 1: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn long_sequential_timers(num_tasks: usize) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    for i in 1..=num_tasks {
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, i.to_string());
        t.add_full_wf_task();
    }

    t.add_workflow_execution_completed();
    t
}

/// Sends 5 large signals per workflow task
pub fn lots_of_big_signals(num_tasks: usize) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let mut rng = rand::rng();
    for _ in 1..=num_tasks {
        let mut dat = [0_u8; 1024 * 1000];
        for _ in 1..=5 {
            rng.fill_bytes(&mut dat);
            t.add_we_signaled(
                "bigsig",
                vec![Payload {
                    metadata: Default::default(),
                    data: dat.into(),
                }],
            );
        }
        t.add_full_wf_task();
    }

    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_TIMER_STARTED
///  7: EVENT_TYPE_TIMER_FIRED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 12: EVENT_TYPE_TIMER_STARTED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
pub fn unsent_at_cancel_repro() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let scheduled_event_id = t.add_activity_task_scheduled(1.to_string());
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, 1.to_string());

    t.add_full_wf_task();
    t.add_activity_task_cancel_requested(scheduled_event_id);
    t.add_by_type(EventType::TimerStarted);
    t.add_workflow_task_scheduled_and_started();

    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_TIMER_STARTED
/// 12: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 13: EVENT_TYPE_ACTIVITY_TASK_COMPLETED
/// 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 15: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 16: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 17: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn cancel_not_sent_when_also_complete_repro() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let scheduled_event_id = t.add_activity_task_scheduled("act-1");
    t.add_we_signaled(
        "sig-1",
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_full_wf_task();
    t.add_activity_task_cancel_requested(scheduled_event_id);
    t.add_by_type(EventType::TimerStarted);
    let started_event_id = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add(
        history_event::Attributes::ActivityTaskCompletedEventAttributes(
            ActivityTaskCompletedEventAttributes {
                scheduled_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
///  6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED (at-started)
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED (at-completed)
///  9: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 10: EVENT_TYPE_ACTIVITY_TASK_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 12: EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn wft_timeout_repro() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("1");
    t.add_we_signaled(
        "at-started",
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    t.add_workflow_task_scheduled();
    t.add_we_signaled(
        "at-completed",
        vec![Payload {
            metadata: Default::default(),
            data: b"hello ".to_vec(),
        }],
    );
    let started_event_id = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add(ActivityTaskCompletedEventAttributes {
        scheduled_event_id,
        started_event_id,
        ..Default::default()
    });
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
pub fn timer_then_continue_as_new(timer_id: &str) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, timer_id.to_string());
    t.add_full_wf_task();
    t.add_continued_as_new();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
pub fn timer_wf_cancel_req_cancelled(timer_id: &str) -> TestHistoryBuilder {
    timer_cancel_req_then(timer_id, TestHistoryBuilder::add_cancelled)
}
pub fn timer_wf_cancel_req_completed(timer_id: &str) -> TestHistoryBuilder {
    timer_cancel_req_then(
        timer_id,
        TestHistoryBuilder::add_workflow_execution_completed,
    )
}
pub fn timer_wf_cancel_req_failed(timer_id: &str) -> TestHistoryBuilder {
    timer_cancel_req_then(timer_id, TestHistoryBuilder::add_workflow_execution_failed)
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 11: EVENT_TYPE_TIMER_STARTED
/// 12: EVENT_TYPE_TIMER_FIRED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
pub fn timer_wf_cancel_req_do_another_timer_then_cancelled() -> TestHistoryBuilder {
    timer_cancel_req_then("1", |t| {
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, "2".to_string());
        t.add_full_wf_task();
        t.add_cancelled();
    })
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_TIMER_STARTED
///  6: EVENT_TYPE_TIMER_FIRED
///  7: EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
///  8: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  9: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 10: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// xxxxx
fn timer_cancel_req_then(
    timer_id: &str,
    end_action: impl Fn(&mut TestHistoryBuilder),
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, timer_id.to_string());
    t.add_cancel_requested();
    t.add_full_wf_task();
    end_action(&mut t);
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED
///  3: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  4: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  5: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  6: EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
pub fn immediate_wf_cancel() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_cancel_requested();
    t.add_full_wf_task();
    t.add_cancelled();
    t
}

/// 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 3: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 5: EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// 6: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 8: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED
/// 11: EVENT_TYPE_TIMER_STARTED
/// 12: EVENT_TYPE_ACTIVITY_TASK_STARTED
/// 13: EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT
/// 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 15: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
/// 16: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 17: EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT
/// 18: EVENT_TYPE_TIMER_FIRED
/// 19: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 20: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 21: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 22: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn activity_double_resolve_repro() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_sched_id = t.add(
        history_event::Attributes::ActivityTaskScheduledEventAttributes(
            ActivityTaskScheduledEventAttributes {
                activity_id: "1".to_string(),
                ..Default::default()
            },
        ),
    );
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task();
    t.add_activity_task_cancel_requested(act_sched_id);
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id: act_sched_id,
        ..Default::default()
    });
    t.add(
        history_event::Attributes::ActivityTaskTimedOutEventAttributes(
            ActivityTaskTimedOutEventAttributes {
                scheduled_event_id: act_sched_id,
                ..Default::default()
            },
        ),
    );
    t.add_workflow_task_scheduled();
    t.add_we_signaled("sig2", vec![]);
    t.add_workflow_task_started();
    t.add_workflow_task_timed_out();
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
fn start_child_wf_preamble(child_wf_id: &str) -> (TestHistoryBuilder, i64, i64) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let initiated_event_id = t.add(StartChildWorkflowExecutionInitiatedEventAttributes {
        workflow_id: child_wf_id.to_owned(),
        workflow_type: Some("child".into()),
        ..Default::default()
    });
    let started_event_id = t.add(ChildWorkflowExecutionStartedEventAttributes {
        initiated_event_id,
        workflow_execution: Some(WorkflowExecution {
            workflow_id: child_wf_id.to_owned(),
            ..Default::default()
        }),
        ..Default::default()
    });
    t.add_full_wf_task();
    (t, initiated_event_id, started_event_id)
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
/// 11: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 12: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 13: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 14: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow(child_wf_id: &str) -> TestHistoryBuilder {
    let (mut t, initiated_event_id, started_event_id) = start_child_wf_preamble(child_wf_id);
    t.add(
        history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(
            ChildWorkflowExecutionCompletedEventAttributes {
                initiated_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED
/// 11: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 12: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 13: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 14: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow_fail(child_wf_id: &str) -> TestHistoryBuilder {
    let (mut t, initiated_event_id, started_event_id) = start_child_wf_preamble(child_wf_id);
    t.add(
        history_event::Attributes::ChildWorkflowExecutionFailedEventAttributes(
            ChildWorkflowExecutionFailedEventAttributes {
                initiated_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_SIGNAL_WORKFLOW_EXECUTION_INITIATED
/// 11: EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED
/// 12: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow_signaled(child_wf_id: &str, signame: &str) -> TestHistoryBuilder {
    let (mut t, initiated_event_id, started_event_id) = start_child_wf_preamble(child_wf_id);
    let id = t.add_signal_wf(signame, "fake_wid", "fake_rid");
    t.add_external_signal_completed(id);
    t.add(
        history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(
            ChildWorkflowExecutionCompletedEventAttributes {
                initiated_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
/// 11: EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
/// 12: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELLED
/// 13: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 14: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 15: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 16: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow_cancelled(child_wf_id: &str) -> TestHistoryBuilder {
    let (mut t, initiated_event_id, started_event_id) = start_child_wf_preamble(child_wf_id);
    let id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        workflow_id: child_wf_id.to_string(),
        ..Default::default()
    });
    t.add_cancel_external_wf_completed(id);
    t.add(
        history_event::Attributes::ChildWorkflowExecutionCanceledEventAttributes(
            ChildWorkflowExecutionCanceledEventAttributes {
                initiated_event_id,
                started_event_id,
                ..Default::default()
            },
        ),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow_abandon_cancelled(child_wf_id: &str) -> TestHistoryBuilder {
    let (mut t, _, _) = start_child_wf_preamble(child_wf_id);
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED
///  6: EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED
///  7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  8: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  9: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 10: EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED
/// 11: EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED
/// 12: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// 13: EVENT_TYPE_WORKFLOW_TASK_STARTED
/// 14: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// 15: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn single_child_workflow_try_cancelled(child_wf_id: &str) -> TestHistoryBuilder {
    let (mut t, _, _) = start_child_wf_preamble(child_wf_id);
    let id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        workflow_id: child_wf_id.to_string(),
        ..Default::default()
    });
    t.add_cancel_external_wf_completed(id);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

///  1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
///  2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
///  3: EVENT_TYPE_WORKFLOW_TASK_STARTED
///  4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
///  5: EVENT_TYPE_MARKER_RECORDED (la result)
///  7: EVENT_TYPE_MARKER_RECORDED (la result)
///  8: EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
pub fn two_local_activities_one_wft(parallel: bool) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let mut start_time = t.wft_start_time();
    start_time.seconds += 1;
    t.add_local_activity_result_marker_with_time(1, "1", b"hi".into(), start_time);
    if !parallel {
        start_time.seconds += 1;
    }
    t.add_local_activity_result_marker_with_time(2, "2", b"hi2".into(), start_time);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// Useful for one-of needs to write a crafted history to a file. Writes it as serialized proto
/// binary to the provided path.
pub fn write_hist_to_binfile(
    thb: &TestHistoryBuilder,
    file_path: PathBuf,
) -> Result<(), anyhow::Error> {
    let as_complete_hist: History = thb.get_full_history_info()?.into();
    let serialized = as_complete_hist.encode_to_vec();
    let mut file = File::create(file_path)?;
    file.write_all(&serialized)?;
    Ok(())
}
