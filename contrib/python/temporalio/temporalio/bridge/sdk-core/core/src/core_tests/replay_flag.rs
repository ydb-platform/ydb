use crate::{
    test_help::{
        MockPollCfg, ResponseType, build_fake_sdk, build_mock_pollers, canned_histories,
        hist_to_poll_resp, mock_worker,
    },
    worker::{LEGACY_QUERY_ID, client::mocks::mock_worker_client},
};
use rstest::{fixture, rstest};
use std::{collections::VecDeque, time::Duration};
use temporal_sdk::{WfContext, Worker, WorkflowFunction};
use temporal_sdk_core_api::Worker as CoreWorker;
use temporal_sdk_core_protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{enums::v1::EventType, query::v1::WorkflowQuery},
};
use temporal_sdk_core_test_utils::{
    interceptors::ActivationAssertionsInterceptor, query_ok, start_timer_cmd,
};

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |command_sink: WfContext| async move {
        for _ in 1..=num_timers {
            command_sink.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}

#[fixture(num_timers = 1)]
fn fire_happy_hist(num_timers: u32) -> Worker {
    let func = timers_wf(num_timers);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker
}

#[rstest]
#[case::one_timer(fire_happy_hist(1), 1)]
#[case::five_timers(fire_happy_hist(5), 5)]
#[tokio::test]
async fn replay_flag_is_correct(#[case] mut worker: Worker, #[case] num_timers: usize) {
    // Verify replay flag is correct by constructing a workflow manager that already has a complete
    // history fed into it. It should always be replaying, because history is complete.

    let mut aai = ActivationAssertionsInterceptor::default();

    for _ in 1..=num_timers + 1 {
        aai.then(|a| assert!(a.is_replaying));
    }

    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_flag_is_correct_partial_history() {
    let func = timers_wf(1);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(2);
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [1]));
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(|a| assert!(!a.is_replaying));

    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}

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
