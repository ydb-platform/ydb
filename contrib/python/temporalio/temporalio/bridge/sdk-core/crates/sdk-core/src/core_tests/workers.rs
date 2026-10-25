use crate::{
    CompleteNexusError, PollError, prost_dur,
    replay::canned_histories,
    test_help::{
        MockPollCfg, MockWorkerInputs, MocksHolder, QueueResponse, ResponseType, WorkerExt,
        WorkerTestHelpers, build_fake_worker, build_mock_pollers, mock_worker, start_timer_cmd,
        test_worker_cfg,
    },
    worker::{
        self, PollerBehavior,
        client::{
            MockWorkerClient,
            mocks::{DEFAULT_TEST_CAPABILITIES, mock_worker_client},
        },
    },
};
use futures_util::{stream, stream::StreamExt};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::worker::ClientWorkerSet;
use temporalio_common::{
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            nexus::{NexusOperationErrorState, NexusTaskCompletion, nexus_task_completion},
            workflow_activation::workflow_activation_job,
            workflow_commands::{CompleteWorkflowExecution, StartTimer, workflow_command},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::{ActivityType, WorkflowExecution},
            enums::v1::NexusHandlerErrorRetryBehavior,
            failure::v1::{
                ApplicationFailureInfo, CanceledFailureInfo, Failure, NexusHandlerFailureInfo,
                TimeoutFailureInfo, failure::FailureInfo,
            },
            namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
            nexus::{
                self,
                v1::{
                    HandlerError, NexusTaskFailure, Request as NexusRequest,
                    Response as NexusResponse, StartOperationRequest, StartOperationResponse,
                    UnsuccessfulOperationError, response, start_operation_response,
                },
            },
            workflowservice::v1::{
                DescribeNamespaceResponse, PollActivityTaskQueueResponse,
                PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
                RespondActivityTaskCompletedResponse, RespondNexusTaskCompletedResponse,
                RespondNexusTaskFailedResponse, RespondWorkflowTaskCompletedResponse,
                ShutdownWorkerResponse,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::{Barrier, Notify, watch};
use uuid::Uuid;

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
        w.task_types = WorkerTaskTypes::workflow_only();
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
    let workers = Arc::new(ClientWorkerSet::new());
    mock.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    mock.expect_workers().returning(move || workers.clone());
    mock.expect_is_mock().returning(|| true);
    mock.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    mock.expect_identity()
        .returning(|| "test-identity".to_string());
    mock.expect_worker_grouping_key().returning(Uuid::new_v4);
    mock.expect_worker_instance_key().returning(Uuid::new_v4);
    if api_success {
        mock.expect_shutdown_worker()
            .times(1)
            .returning(|_, _, _, _| Ok(ShutdownWorkerResponse {}));
    } else {
        // worker.shutdown() should succeed even if shutdown_worker fails
        mock.expect_shutdown_worker()
            .times(1)
            .returning(|_, _, _, _| Err(tonic::Status::unavailable("fake shutdown error")));
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

fn create_test_activity_task() -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        task_token: b"act-task".to_vec(),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "test".to_string(),
            run_id: "run-id".to_string(),
        }),
        activity_id: "activity".to_string(),
        activity_type: Some(ActivityType {
            name: "activity".to_string(),
        }),
        ..Default::default()
    }
}

#[allow(deprecated)] // poller_group_infos is deprecated but still an explicit field
fn create_test_nexus_task(
    header: Option<HashMap<String, String>>,
    capabilities: Option<nexus::v1::request::Capabilities>,
) -> PollNexusTaskQueueResponse {
    PollNexusTaskQueueResponse {
        task_token: b"nex-task".to_vec(),
        request: Some(NexusRequest {
            capabilities,
            endpoint: "test-endpoint".to_string(),
            header: header.unwrap_or_default(),
            scheduled_time: None,
            variant: Some(temporalio_common::protos::temporal::api::nexus::v1::request::Variant::StartOperation(
                StartOperationRequest {
                    service: "test-service".to_string(),
                    operation: "test-operation".to_string(),
                    request_id: "test-request-id".to_string(),
                    callback: "".to_string(),
                    payload: None,
                    callback_header: Default::default(),
                    links: vec![],
                }
            )),
        }),
        poller_scaling_decision: None,
        poller_group_id: Default::default(),
        poller_group_infos: vec![],
        poller_groups_info: None,
    }
}

fn create_test_nexus_completion(task_token: &[u8]) -> NexusTaskCompletion {
    NexusTaskCompletion {
        task_token: task_token.to_vec(),
        status: Some(temporalio_common::protos::coresdk::nexus::nexus_task_completion::Status::Completed(
            NexusResponse {
                variant: Some(temporalio_common::protos::temporal::api::nexus::v1::response::Variant::StartOperation(
                    StartOperationResponse {
                        variant: Some(start_operation_response::Variant::SyncSuccess(
                            start_operation_response::Sync {
                                payload: None,
                                links: vec![],
                            }
                        )),
                    }
                )),
            }
        )),
    }
}

#[rstest::rstest]
// With tasks
#[case::activity_only_with_task(false, false, true, false, true, "activity-only")]
#[case::nexus_only_with_task(false, false, false, true, true, "nexus-only")]
#[case::workflow_only_with_task(true, false, false, false, true, "workflow-only")]
#[case::workflow_and_activity_with_task(true, true, true, false, true, "workflow-activity")]
#[case::workflow_and_local_activity_with_task(
    true,
    true,
    false,
    false,
    true,
    "workflow-local-activity"
)]
#[case::workflow_and_nexus_with_task(true, false, false, true, true, "workflow-nexus")]
#[case::activity_and_nexus_with_task(false, false, true, true, true, "activity-nexus")]
// Without tasks (idle worker shutdown)
#[case::activity_only_idle(false, false, true, false, false, "activity-only-idle")]
#[case::nexus_only_idle(false, false, false, true, false, "nexus-only-idle")]
#[case::workflow_only_idle(true, false, false, false, false, "workflow-only-idle")]
#[case::workflow_and_activity_idle(true, true, true, false, false, "workflow-activity-idle")]
#[case::workflow_and_local_activity_idle(
    true,
    true,
    false,
    false,
    false,
    "workflow-local-activity-idle"
)]
#[case::workflow_and_nexus_idle(true, false, false, true, false, "workflow-nexus-idle")]
#[case::activity_and_nexus_idle(false, false, true, true, false, "activity-nexus-idle")]
#[tokio::test]
async fn test_task_type_combinations_unified(
    #[case] enable_workflows: bool,
    #[case] enable_local_activities: bool,
    #[case] enable_remote_activities: bool,
    #[case] enable_nexus: bool,
    #[case] with_task: bool,
    #[case] queue_name: &str,
) {
    let mut client = mock_worker_client();

    if with_task {
        if enable_local_activities || enable_remote_activities {
            client
                .expect_complete_activity_task()
                .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
        }
        if enable_nexus {
            client
                .expect_complete_nexus_task()
                .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
        }
    }

    let mut mocks = if enable_workflows && with_task {
        let t = canned_histories::single_timer(queue_name);
        let wf_cfg = MockPollCfg::from_resp_batches(queue_name, t, [1], client);
        let mut mocks = build_mock_pollers(wf_cfg);
        if enable_remote_activities {
            mocks.set_act_poller_from_resps(vec![QueueResponse::from(create_test_activity_task())]);
        }
        if enable_nexus {
            mocks.set_nexus_poller_from_resps(vec![QueueResponse::from(create_test_nexus_task(
                None,
                Some(nexus::v1::request::Capabilities {
                    temporal_failure_responses: true,
                }),
            ))]);
        }
        mocks
    } else {
        let wft_stream = if enable_workflows {
            Some(stream::empty())
        } else {
            None
        };
        let activity_tasks = if enable_remote_activities && with_task {
            Some(vec![QueueResponse::from(create_test_activity_task())])
        } else {
            None
        };
        let nexus_tasks = if enable_nexus && with_task {
            Some(vec![QueueResponse::from(create_test_nexus_task(
                None,
                Some(nexus::v1::request::Capabilities {
                    temporal_failure_responses: true,
                }),
            ))])
        } else {
            None
        };
        MocksHolder::from_client_with_custom(client, wft_stream, activity_tasks, nexus_tasks)
    };

    mocks.worker_cfg(|w| {
        w.task_queue = queue_name.to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows,
            enable_local_activities,
            enable_remote_activities,
            enable_nexus,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    if with_task {
        if enable_workflows {
            let activation = worker.poll_workflow_activation().await.unwrap();
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                    activation.run_id.clone(),
                    vec![CompleteWorkflowExecution::default().into()],
                ))
                .await
                .unwrap();
        }

        if enable_remote_activities {
            let activity_task = worker.poll_activity_task().await.unwrap();
            worker
                .complete_activity_task(ActivityTaskCompletion {
                    task_token: activity_task.task_token,
                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                })
                .await
                .unwrap();
        }

        if enable_nexus {
            let nexus_task = worker.poll_nexus_task().await.unwrap();
            worker
                .complete_nexus_task(create_test_nexus_completion(nexus_task.task_token()))
                .await
                .unwrap();
        }
    }

    worker.initiate_shutdown();
    if enable_workflows {
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    if enable_local_activities || enable_remote_activities {
        assert_matches!(
            worker.poll_activity_task().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    if enable_nexus {
        assert_matches!(
            worker.poll_nexus_task().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_request_deadline_missing_header() {
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
    let nexus_task = create_test_nexus_task(None, None); // No headers
    let mut mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<_>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(nexus_task)]),
    );
    mocks.worker_cfg(|w| {
        w.task_queue = "test-queue".to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: true,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    assert!(
        nexus_task.request_deadline.is_none(),
        "request_deadline should be None when header is missing"
    );

    worker
        .complete_nexus_task(create_test_nexus_completion(nexus_task.task_token()))
        .await
        .unwrap();
    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_request_deadline_valid_header() {
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
    let mut headers = std::collections::HashMap::new();
    headers.insert("request-timeout".to_string(), "30s".to_string());
    let nexus_task = create_test_nexus_task(Some(headers), None);
    let mut mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<_>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(nexus_task)]),
    );
    mocks.worker_cfg(|w| {
        w.task_queue = "test-queue".to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: true,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let before = std::time::SystemTime::now();
    let nexus_task = worker.poll_nexus_task().await.unwrap();
    let after = std::time::SystemTime::now();

    assert!(
        nexus_task.request_deadline.is_some(),
        "request_deadline should be Some when valid header is present"
    );
    let deadline: std::time::SystemTime = nexus_task.request_deadline.unwrap().try_into().unwrap();
    // Deadline should be approximately 30s from now
    let expected_min = before + Duration::from_secs(30);
    let expected_max = after + Duration::from_secs(30);
    assert!(
        deadline >= expected_min && deadline <= expected_max,
        "deadline {:?} should be between {:?} and {:?}",
        deadline,
        expected_min,
        expected_max
    );

    worker
        .complete_nexus_task(create_test_nexus_completion(nexus_task.task_token()))
        .await
        .unwrap();
    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_request_deadline_invalid_header() {
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
    let mut headers = std::collections::HashMap::new();
    headers.insert("request-timeout".to_string(), "invalid".to_string());
    let nexus_task = create_test_nexus_task(Some(headers), None);
    let mut mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<_>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(nexus_task)]),
    );
    mocks.worker_cfg(|w| {
        w.task_queue = "test-queue".to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: true,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    assert!(
        nexus_task.request_deadline.is_none(),
        "request_deadline should be None when header is invalid"
    );

    worker
        .complete_nexus_task(create_test_nexus_completion(nexus_task.task_token()))
        .await
        .unwrap();
    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_task_completion_with_failure_status() {
    let mut client = mock_worker_client();
    client
        .expect_fail_nexus_task()
        .times(1)
        .withf(|_, error| {

            matches!(error,
                NexusTaskFailure::Temporal(Failure{ message, failure_info, .. })
                if message == "handler failed" &&
                matches!(failure_info,
                    Some(FailureInfo::NexusHandlerFailureInfo(NexusHandlerFailureInfo{ r#type, retry_behavior }))
                    if r#type == "INTERNAL" &&
                    *retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32
                )
            )

        })
        .returning(|_, _| Ok(RespondNexusTaskFailedResponse::default()));

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(nexus_task_completion::Status::Failure(Failure {
                message: "handler failed".to_string(),
                failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                    NexusHandlerFailureInfo {
                        r#type: "INTERNAL".to_string(),
                        retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                    },
                )),
                ..Default::default()
            })),
        })
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_task_completion_with_failure_converts_to_legacy_for_old_server() {
    let mut client = mock_worker_client();
    client
        .expect_fail_nexus_task()
        .times(1)
        .withf(|_, error| {
            matches!(error,
                NexusTaskFailure::Legacy(
                    HandlerError{
                        error_type,
                        failure: Some(f),
                        retry_behavior,
                    }
                )
                if error_type == "INTERNAL" &&
                f.message == "handler failed" &&
                *retry_behavior == NexusHandlerErrorRetryBehavior::NonRetryable as i32
            )
        })
        .returning(|_, _| Ok(RespondNexusTaskFailedResponse::default()));

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: false,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(nexus_task_completion::Status::Failure(Failure {
                message: "handler failed".to_string(),
                failure_info: Some(FailureInfo::NexusHandlerFailureInfo(
                    NexusHandlerFailureInfo {
                        r#type: "INTERNAL".to_string(),
                        retry_behavior: NexusHandlerErrorRetryBehavior::NonRetryable.into(),
                    },
                )),
                ..Default::default()
            })),
        })
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[rstest::rstest]
#[case::with_temporal_failure_responses(true)]
#[case::without_temporal_failure_responses(false)]
#[tokio::test]
async fn nexus_task_completion_with_failure_status_missing_handler_info_fails(
    #[case] temporal_failure_responses: bool,
) {
    let client = mock_worker_client();

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    let result = worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(nexus_task_completion::Status::Failure(Failure {
                message: "handler failed".to_string(),
                // Missing NexusHandlerFailureInfo - should fail validation
                failure_info: None,
                ..Default::default()
            })),
        })
        .await;

    // Should fail validation because NexusHandlerFailureInfo is required
    assert_matches!(
        result,
        Err(CompleteNexusError::MalformedNexusCompletion { reason }) if reason.contains("NexusHandlerFailureInfo")
    );

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

fn create_nexus_completion_with_start_op_failure(
    task_token: &[u8],
    failure: Failure,
) -> NexusTaskCompletion {
    NexusTaskCompletion {
        task_token: task_token.to_vec(),
        status: Some(nexus_task_completion::Status::Completed(NexusResponse {
            variant: Some(response::Variant::StartOperation(StartOperationResponse {
                variant: Some(start_operation_response::Variant::Failure(failure)),
            })),
        })),
    }
}

#[tokio::test]
async fn nexus_start_operation_failure_with_application_failure_info() {
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .times(1)
        .withf(|_, response| {
            matches!(response,
                nexus::v1::Response{
                    variant: Some(response::Variant::StartOperation(StartOperationResponse {
                        variant: Some(start_operation_response::Variant::Failure(
                            Failure {
                                message,
                                failure_info: Some(FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo { r#type, non_retryable, ..})),
                                .. }
                        ))
                    }))
                }
                if message == "operation failed" &&
                r#type == "MyAppError" &&
                *non_retryable
            )
        })
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(create_nexus_completion_with_start_op_failure(
            nexus_task.task_token(),
            Failure {
                message: "operation failed".to_string(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: "MyAppError".to_string(),
                        non_retryable: true,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        ))
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn nexus_start_operation_failure_with_canceled_failure_info() {
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .times(1)
        .withf(|_, response| {
            matches!(response,
                nexus::v1::Response{
                    variant: Some(response::Variant::StartOperation(StartOperationResponse {
                        variant: Some(start_operation_response::Variant::Failure(
                            Failure {
                                message,
                                failure_info: Some(FailureInfo::CanceledFailureInfo(_)),
                                .. }
                        ))
                    }))
                }
                if message == "operation canceled"
            )
        })
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(create_nexus_completion_with_start_op_failure(
            nexus_task.task_token(),
            Failure {
                message: "operation canceled".to_string(),
                failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                    details: None,
                    identity: Default::default(),
                })),
                ..Default::default()
            },
        ))
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[rstest::rstest]
#[case::missing_failure_info(None)]
#[case::invalid_failure_info_type(Some(FailureInfo::TimeoutFailureInfo(
    TimeoutFailureInfo::default()
)))]
#[tokio::test]
async fn nexus_start_operation_failure_with_invalid_failure_info(
    #[case] failure_info: Option<FailureInfo>,
) {
    let client = mock_worker_client();

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: true,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    let result = worker
        .complete_nexus_task(create_nexus_completion_with_start_op_failure(
            nexus_task.task_token(),
            Failure {
                message: "operation failed".to_string(),
                failure_info,
                ..Default::default()
            },
        ))
        .await;

    assert_matches!(
        result,
        Err(CompleteNexusError::MalformedNexusCompletion { reason })
            if reason.contains("ApplicationFailureInfo") && reason.contains("CanceledFailureInfo")
    );

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[rstest::rstest]
#[case::application_failure(
    FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
        r#type: "MyAppError".to_string(),
        non_retryable: true,
        ..Default::default()
    }),
    NexusOperationErrorState::Failed
)]
#[case::canceled_failure(
    FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
        details: None,
        identity: Default::default(),
    }),
    NexusOperationErrorState::Canceled
)]
#[tokio::test]
async fn nexus_start_operation_failure_converts_to_legacy_for_old_server(
    #[case] failure_info: FailureInfo,
    #[case] expected_operation_state: NexusOperationErrorState,
) {
    let expected_state = expected_operation_state.to_string();
    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .times(1)
        .withf(move |_, response| {
            matches!(response,
                nexus::v1::Response{
                    variant: Some(response::Variant::StartOperation(StartOperationResponse {
                        #[allow(deprecated)]
                        variant: Some(
                            start_operation_response::Variant::OperationError(
                            UnsuccessfulOperationError{
                                operation_state,
                                failure: Some(nexus::v1::Failure{
                                    message,
                                    metadata,
                                    ..}),
                            }
                        ))
                    }))
                }
                if message == "operation failed" &&
                operation_state.as_str() == expected_state &&
                metadata.get("type").unwrap() == "temporal.api.failure.v1.Failure"
            )
        })
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));

    let mocks = MocksHolder::from_client_with_custom(
        client,
        None::<stream::Empty<PollWorkflowTaskQueueResponse>>,
        None::<Vec<QueueResponse<PollActivityTaskQueueResponse>>>,
        Some(vec![QueueResponse::from(create_test_nexus_task(
            None,
            Some(nexus::v1::request::Capabilities {
                temporal_failure_responses: false,
            }),
        ))]),
    );
    let worker = mock_worker(mocks);

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(create_nexus_completion_with_start_op_failure(
            nexus_task.task_token(),
            Failure {
                message: "operation failed".to_string(),
                failure_info: Some(failure_info),
                ..Default::default()
            },
        ))
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

/// Verifies that `initiate_shutdown` sends the `ShutdownWorker` RPC so that the server can
/// complete in-flight polls. Without this, graceful poll shutdown deadlocks: the SDK waits for
/// polls to drain, but the server was never told to flush them.
#[tokio::test]
async fn graceful_shutdown_sends_shutdown_worker_rpc_during_initiate() {
    let shutdown_rpc_called = Arc::new(AtomicBool::new(false));
    let shutdown_rpc_called_clone = shutdown_rpc_called.clone();
    // When the shutdown_worker RPC fires, it signals polls to complete (simulating server
    // behavior where ShutdownWorker causes the server to return empty poll responses).
    let poll_releaser = Arc::new(Notify::new());
    let poll_releaser_for_rpc = poll_releaser.clone();

    let mut mock_client = MockWorkerClient::new();
    let workers = Arc::new(ClientWorkerSet::new());
    mock_client
        .expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    mock_client
        .expect_workers()
        .returning(move || workers.clone());
    mock_client.expect_is_mock().returning(|| true);
    mock_client
        .expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    mock_client
        .expect_identity()
        .returning(|| "test-identity".to_string());
    mock_client
        .expect_worker_grouping_key()
        .returning(Uuid::new_v4);
    mock_client
        .expect_worker_instance_key()
        .returning(Uuid::new_v4);
    mock_client
        .expect_set_heartbeat_client_fields()
        .returning(|hb| {
            hb.sdk_name = "test-core".to_string();
            hb.sdk_version = "0.0.0".to_string();
            hb.worker_identity = "test-identity".to_string();
            hb.heartbeat_time = Some(std::time::SystemTime::now().into());
        });
    // Return the worker_poll_complete_on_shutdown capability so graceful mode is enabled.
    mock_client.expect_describe_namespace().returning(move || {
        Ok(DescribeNamespaceResponse {
            namespace_info: Some(NamespaceInfo {
                capabilities: Some(Capabilities {
                    worker_poll_complete_on_shutdown: true,
                    ..Capabilities::default()
                }),
                ..NamespaceInfo::default()
            }),
            ..DescribeNamespaceResponse::default()
        })
    });
    // When shutdown_worker RPC is called, mark it and release polls
    mock_client
        .expect_shutdown_worker()
        .returning(move |_, _, _, _| {
            shutdown_rpc_called_clone.store(true, Ordering::SeqCst);
            poll_releaser_for_rpc.notify_waiters();
            Ok(ShutdownWorkerResponse {})
        });
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));

    // Polls block until shutdown_worker RPC releases them (simulating server holding polls
    // open until it receives the ShutdownWorker signal)
    let poll_releaser_for_stream = poll_releaser.clone();
    let stream = stream::unfold(poll_releaser_for_stream, |releaser| async move {
        releaser.notified().await;
        Some((
            Ok(PollWorkflowTaskQueueResponse::default().try_into().unwrap()),
            releaser,
        ))
    });

    let mw = MockWorkerInputs::new(stream.boxed());
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));

    // validate() reads describe_namespace and sets capabilities.graceful_poll_shutdown = true
    worker.validate().await.unwrap();

    let poll_fut = worker.poll_workflow_activation();
    let shutdown_fut = async {
        // initiate_shutdown must send the ShutdownWorker RPC, which releases the polls
        worker.initiate_shutdown();
    };

    let (poll_result, _) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(poll_fut, shutdown_fut)
    })
    .await
    .expect("Shutdown should complete within 5s -- if it hangs, the ShutdownWorker RPC was not sent during initiate_shutdown");

    assert_matches!(poll_result.unwrap_err(), PollError::ShutDown);
    assert!(
        shutdown_rpc_called.load(Ordering::SeqCst),
        "ShutdownWorker RPC must be called during initiate_shutdown"
    );

    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn validate_enables_auto_enroll_capability() {
    let mut mock = mock_worker_client();
    mock.expect_describe_namespace().returning(|| {
        Ok(DescribeNamespaceResponse {
            namespace_info: Some(NamespaceInfo {
                capabilities: Some(Capabilities {
                    poller_autoscaling_auto_enroll: true,
                    ..Capabilities::default()
                }),
                ..NamespaceInfo::default()
            }),
            ..DescribeNamespaceResponse::default()
        })
    });
    let t = canned_histories::single_timer("1");
    let mut mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    mh.enforce_correct_number_of_polls = false;
    let worker = mock_worker(build_mock_pollers(mh));

    worker.validate().await.unwrap();
    let caps = worker.get_namespace_capabilities();
    assert!(caps.poller_autoscaling_auto_enroll());
    // The two capabilities are independent: advertising auto-enroll alone does not imply the
    // `poller_autoscaling` capability (the server advertises each on its own).
    assert!(!caps.poller_autoscaling());

    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn validate_without_auto_enroll_leaves_capabilities_off() {
    let mut mock = mock_worker_client();
    mock.expect_describe_namespace().returning(|| {
        Ok(DescribeNamespaceResponse {
            namespace_info: Some(NamespaceInfo {
                capabilities: Some(Capabilities {
                    poller_autoscaling_auto_enroll: false,
                    ..Capabilities::default()
                }),
                ..NamespaceInfo::default()
            }),
            ..DescribeNamespaceResponse::default()
        })
    });
    let t = canned_histories::single_timer("1");
    let mut mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    mh.enforce_correct_number_of_polls = false;
    let worker = mock_worker(build_mock_pollers(mh));

    worker.validate().await.unwrap();
    let caps = worker.get_namespace_capabilities();
    assert!(!caps.poller_autoscaling_auto_enroll());
    assert!(!caps.poller_autoscaling());

    worker.drain_pollers_and_shutdown().await;
}
