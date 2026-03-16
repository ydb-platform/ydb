use crate::{
    PollError, prost_dur,
    test_help::{
        MockPollCfg, MockWorkerInputs, MocksHolder, ResponseType, WorkerExt, build_fake_worker,
        build_mock_pollers, canned_histories, mock_worker, test_worker_cfg,
    },
    worker::{
        self,
        client::{
            MockWorkerClient,
            mocks::{DEFAULT_TEST_CAPABILITIES, DEFAULT_WORKERS_REGISTRY, mock_worker_client},
        },
    },
};
use futures_util::{stream, stream::StreamExt};
use std::{cell::RefCell, time::Duration};
use temporal_sdk_core_api::{Worker, worker::PollerBehavior};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::workflow_activation_job,
        workflow_commands::{CompleteWorkflowExecution, StartTimer, workflow_command},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::workflowservice::v1::{
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse, ShutdownWorkerResponse,
    },
};
use temporal_sdk_core_test_utils::{WorkerTestHelpers, start_timer_cmd};
use tokio::sync::{Barrier, watch};

#[tokio::test]
async fn after_shutdown_of_worker_get_shutdown_err() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, [1]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(worker.shutdown(), async {
        // Need to complete task for shutdown to finish
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                workflow_command::Variant::StartTimer(StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
                }),
            ))
            .await
            .unwrap();

        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}

#[tokio::test]
async fn shutdown_worker_can_complete_pending_activation() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, [2]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Complete the timer, will queue PA
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();

    tokio::join!(worker.shutdown(), async {
        let res = worker.poll_workflow_activation().await.unwrap();
        // The timer fires
        assert_eq!(res.jobs.len(), 1);
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                res.run_id,
                vec![CompleteWorkflowExecution::default().into()],
            ))
            .await
            .unwrap();
        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}

#[tokio::test]
async fn worker_shutdown_during_poll_doesnt_deadlock() {
    let (tx, rx) = watch::channel(false);
    let rx = rx.clone();
    let stream = stream::unfold(rx, |mut rx| async move {
        // Don't resolve polls until worker shuts down
        rx.changed().await.unwrap();
        // We don't want to return a real response here because it would get buffered and
        // then we'd have real work to do to be able to finish shutdown.
        Some((
            Ok(PollWorkflowTaskQueueResponse::default().try_into().unwrap()),
            rx,
        ))
    });
    let mw = MockWorkerInputs::new(stream.boxed());
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let pollfut = worker.poll_workflow_activation();
    let shutdownfut = async {
        worker.shutdown().await;
        // Either the send works and unblocks the poll or the poll future is dropped before actually
        // polling -- either way things worked OK
        let _ = tx.send(true);
    };
    let (pollres, _) = tokio::join!(pollfut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollError::ShutDown);
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn can_shutdown_local_act_only_worker_when_act_polling() {
    let t = canned_histories::single_timer("1");
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| {
        w.no_remote_activities = true;
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);
    let barrier = Barrier::new(2);

    tokio::join!(
        async {
            barrier.wait().await;
            worker.shutdown().await;
        },
        async {
            let res = worker.poll_workflow_activation().await.unwrap();
            // Complete so there's no outstanding WFT and shutdown can finish
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            barrier.wait().await;
            // We need to see workflow poll return shutdown before activity poll will
            assert_matches!(
                worker.poll_workflow_activation().await.unwrap_err(),
                PollError::ShutDown
            );
            assert_matches!(
                worker.poll_activity_task().await.unwrap_err(),
                PollError::ShutDown
            );
        }
    );
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn complete_with_task_not_found_during_shutdown() {
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::not_found("Workflow task not found.")));
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let core = mock_worker(build_mock_pollers(mh));

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);

    let complete_order = RefCell::new(vec![]);
    // Initiate shutdown before completing the activation
    let shutdown_fut = async {
        core.shutdown().await;
        complete_order.borrow_mut().push(2);
    };
    let poll_fut = async {
        // This will return shutdown once the completion goes through
        assert_matches!(
            core.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    };
    let complete_fut = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();
        complete_order.borrow_mut().push(1);
    };
    tokio::join!(shutdown_fut, poll_fut, complete_fut);
    assert_eq!(&complete_order.into_inner(), &[1, 2])
}

#[tokio::test]
async fn complete_eviction_after_shutdown_doesnt_panic() {
    let t = canned_histories::single_timer("1");
    let mut mh = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [1],
        mock_worker_client(),
    ));
    mh.make_wft_stream_interminable();
    let core = mock_worker(mh);

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    let res = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        res.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    );
    core.shutdown().await;
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_does_not_panic_on_retry_exhaustion_of_nonfatal_net_err() {
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    // Return a failure that counts as retryable, and hence we want to be swallowed
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::internal("Some retryable error")));
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", t, [1.into(), ResponseType::AllHistory], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // This should not return a fatal error
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    // We should see an eviction
    core.handle_eviction().await;
}

#[rstest::rstest]
#[tokio::test]
async fn worker_can_shutdown_after_never_polling_ok(#[values(true, false)] poll_workflow: bool) {
    let mut mock = mock_worker_client();
    mock.expect_poll_activity_task()
        .returning(|_, _| Err(tonic::Status::permission_denied("you shall not pass")));
    if poll_workflow {
        mock.expect_poll_workflow_task()
            .returning(|_, _| Err(tonic::Status::permission_denied("you shall not pass")));
    }
    let core = worker::Worker::new_test(
        test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .unwrap(),
        mock,
    );

    loop {
        // Must continue polling until polls return shutdown.
        if poll_workflow {
            let res = core.poll_workflow_activation().await.unwrap_err();
            if !matches!(res, PollError::ShutDown) {
                continue;
            }
        }
        let res = core.poll_activity_task().await.unwrap_err();
        if !matches!(res, PollError::ShutDown) {
            continue;
        }
        core.finalize_shutdown().await;
        break;
    }
}

#[rstest::rstest]
#[case::ok(true, true)]
#[case::best_effort(true, false)]
#[case::not_sticky(false, true)]
#[tokio::test]
async fn worker_shutdown_api(#[case] use_cache: bool, #[case] api_success: bool) {
    // Manually need to create MockWorkerClient because we want to specify
    // the expected number of calls for shutdown_worker.
    // This will no longer be needed if
    // https://github.com/asomers/mockall/issues/283 is implemented.
    let mut mock = MockWorkerClient::new();
    mock.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    mock.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    mock.expect_is_mock().returning(|| true);
    mock.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    mock.expect_get_identity()
        .returning(|| "test-identity".to_string());
    if use_cache {
        if api_success {
            mock.expect_shutdown_worker()
                .times(1)
                .returning(|_| Ok(ShutdownWorkerResponse {}));
        } else {
            // worker.shutdown() should succeed even if shutdown_worker fails
            mock.expect_shutdown_worker()
                .times(1)
                .returning(|_| Err(tonic::Status::unavailable("fake shutdown error")));
        }
    } else {
        mock.expect_shutdown_worker().times(0);
    }

    let t = canned_histories::single_timer("1");
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", t, [1.into(), ResponseType::AllHistory], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| w.max_cached_workflows = if use_cache { 1 } else { 0 });
    let worker = mock_worker(mock);

    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(worker.shutdown(), async {
        // Need to complete task for shutdown to finish
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                workflow_command::Variant::StartTimer(StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
                }),
            ))
            .await
            .unwrap();

        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}
