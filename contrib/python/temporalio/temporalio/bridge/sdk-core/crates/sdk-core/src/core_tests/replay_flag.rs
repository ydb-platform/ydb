use crate::{
    replay::{TestHistoryBuilder, canned_histories},
    test_help::{
        MockPollCfg, ResponseType, build_mock_pollers, hist_to_poll_resp, mock_worker, query_ok,
        start_timer_cmd,
    },
    worker::{LEGACY_QUERY_ID, client::mocks::mock_worker_client},
};
use std::{collections::VecDeque, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{enums::v1::EventType, query::v1::WorkflowQuery},
};

#[tokio::test]
async fn replay_flag_correct_with_query() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = VecDeque::from(vec![
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 2.into());
            // Server can issue queries that contain the WFT completion and the subsequent
            // commands, but not the consequences yet.
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            let h = pr.history.as_mut().unwrap();
            h.events.truncate(5);
            pr.started_event_id = 3;
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into()),
    ]);
    let mut mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mock.num_expected_legacy_query_resps = 1;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID, "hi"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
}

#[tokio::test]
async fn replay_flag_correct_signal_before_query_ending_on_wft_completed() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("signal", vec![]);
    t.add_full_wf_task();
    let task = {
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory);
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr
    };

    let mut mock = MockPollCfg::from_resp_batches(wfid, t, [task], mock_worker_client());
    mock.num_expected_legacy_query_resps = 1;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID, "hi"),
    ))
    .await
    .unwrap();
}
