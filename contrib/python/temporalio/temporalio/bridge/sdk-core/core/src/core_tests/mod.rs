mod activity_tasks;
mod child_workflows;
mod determinism;
mod local_activities;
mod queries;
mod replay_flag;
mod updates;
mod workers;
mod workflow_cancels;
mod workflow_tasks;

use crate::{
    Worker,
    errors::PollError,
    test_help::{
        MockPollCfg, build_mock_pollers, canned_histories, mock_worker, single_hist_mock_sg,
        test_worker_cfg,
    },
    worker::client::mocks::{mock_manual_worker_client, mock_worker_client},
};
use futures_util::FutureExt;
use std::{sync::LazyLock, time::Duration};
use temporal_sdk_core_api::{Worker as WorkerTrait, worker::PollerBehavior};
use temporal_sdk_core_protos::{
    TestHistoryBuilder,
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::EventType, history::v1::WorkflowExecutionOptionsUpdatedEventAttributes,
    },
};
use tokio::{sync::Barrier, time::sleep};

#[tokio::test]
async fn after_shutdown_server_is_not_polled() {
    let t = canned_histories::single_timer("fake_timer");
    let mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1], mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    // Just so we don't have to deal w/ cache overflow
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
        .await
        .unwrap();
    worker.shutdown().await;
    assert_matches!(
        worker.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.finalize_shutdown().await;
}

// Better than cloning a billion arcs...
static BARR: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(3));

#[tokio::test]
async fn shutdown_interrupts_both_polls() {
    let mut mock_client = mock_manual_worker_client();
    mock_client
        .expect_poll_activity_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });
    mock_client
        .expect_poll_workflow_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });

    let worker = Worker::new_test(
        test_worker_cfg()
            // Need only 1 concurrent pollers for mock expectations to work here
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .unwrap(),
        mock_client,
    );
    tokio::join! {
        async {
            assert_matches!(worker.poll_activity_task().await.unwrap_err(),
                            PollError::ShutDown);
        },
        async {
            assert_matches!(worker.poll_workflow_activation().await.unwrap_err(),
                            PollError::ShutDown);
        },
        async {
            // Give polling a bit to get stuck, then shutdown
            BARR.wait().await;
            worker.shutdown().await;
        }
    };
}

#[tokio::test]
async fn ignores_workflow_options_updated_event() {
    temporal_sdk_core_test_utils::init_integ_telem();

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add(WorkflowExecutionOptionsUpdatedEventAttributes::default());
    t.last_event().unwrap().worker_may_ignore = true;
    t.add_full_wf_task();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg("whatever", t, [1], mock, true);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
}
