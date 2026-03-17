use crate::{
    prost_dur,
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, default_wes_attribs},
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, build_mock_pollers, hist_to_poll_resp, mock_sdk,
        mock_sdk_cfg, mock_worker, single_hist_mock_sg,
    },
    worker::{LEGACY_QUERY_ID, client::mocks::mock_worker_client},
};
use anyhow::anyhow;
use crossbeam_queue::SegQueue;
use futures_util::{FutureExt, future::join_all};
use std::{
    collections::HashMap,
    ops::Sub,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{
    ActContext, ActivityError, LocalActivityOptions, WfContext, WorkflowFunction, WorkflowResult,
};
use temporal_sdk_core_api::{Worker, errors::PollError};
use temporal_sdk_core_protos::{
    DEFAULT_ACTIVITY_TYPE,
    coresdk::{
        ActivityTaskCompletion, AsJsonPayloadExt,
        activity_result::ActivityExecutionResult,
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{ActivityCancellationType, ScheduleLocalActivity},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::RetryPolicy,
        enums::v1::{CommandType, EventType, TimeoutType, WorkflowTaskFailedCause},
        failure::v1::{Failure, failure::FailureInfo},
        query::v1::WorkflowQuery,
    },
};
use temporal_sdk_core_test_utils::{
    WorkerTestHelpers, query_ok, schedule_local_activity_cmd, start_timer_cmd,
};
use tokio::{join, select, sync::Barrier};

async fn echo(_ctx: ActContext, e: String) -> Result<String, ActivityError> {
    Ok(e)
}

/// This test verifies that when replaying we are able to resolve local activities whose data we
/// don't see until after the workflow issues the command
#[rstest::rstest]
#[case::replay(true, true)]
#[case::not_replay(false, true)]
#[case::replay_cache_off(true, false)]
#[case::not_replay_cache_off(false, false)]
#[tokio::test]
async fn local_act_two_wfts_before_marker(#[case] replay: bool, #[case] cached: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"echo".into());
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let resps = if replay {
        vec![ResponseType::AllHistory]
    } else {
        vec![1.into(), 2.into(), ResponseType::AllHistory]
    };
    let mh = MockPollCfg::from_resp_batches(wf_id, t, resps, mock);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if cached {
            cfg.max_cached_workflows = 1;
        }
    });

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            ctx.timer(Duration::from_secs(1)).await;
            la.await;
            Ok(().into())
        },
    );
    worker.register_activity(DEFAULT_ACTIVITY_TYPE, echo);
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

pub(crate) async fn local_act_fanout_wf(ctx: WfContext) -> WorkflowResult<()> {
    let las: Vec<_> = (1..=50)
        .map(|i| {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: format!("Hi {i}")
                    .as_json_payload()
                    .expect("serializes fine"),
                ..Default::default()
            })
        })
        .collect();
    ctx.timer(Duration::from_secs(1)).await;
    join_all(las).await;
    Ok(().into())
}

#[tokio::test]
async fn local_act_many_concurrent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    for i in 1..=50 {
        t.add_local_activity_result_marker(i, &i.to_string(), b"echo".into());
    }
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo", echo);
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Verifies that local activities which take more than a workflow task timeout will cause
/// us to issue additional (empty) WFT completions with the force flag on, thus preventing timeout
/// of WFT while the local activity continues to execute.
///
/// The test with shutdown verifies if we call shutdown while the local activity is running that
/// shutdown does not complete until it's finished.
#[rstest::rstest]
#[case::with_shutdown(true)]
#[case::normal_complete(false)]
#[tokio::test]
async fn local_act_heartbeat(#[case] shutdown_middle: bool) {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_full_wf_task();
    // Task created by WFT heartbeat
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2, 2], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut worker = mock_sdk_cfg(mh, |wc| {
        wc.max_cached_workflows = 1;
        wc.max_outstanding_workflow_tasks = Some(1);
    });
    let core = worker.core_worker.clone();

    let shutdown_barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |_ctx: ActContext, str: String| async move {
        if shutdown_middle {
            shutdown_barr.wait().await;
        }
        // Take slightly more than two workflow tasks
        tokio::time::sleep(wft_timeout.mul_f32(2.2)).await;
        Ok(str)
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let (_, runres) = tokio::join!(
        async {
            if shutdown_middle {
                shutdown_barr.wait().await;
                core.shutdown().await;
            }
        },
        worker.run_until_done()
    );
    runres.unwrap();
}

#[rstest::rstest]
#[case::retry_then_pass(true)]
#[case::retry_until_fail(false)]
#[tokio::test]
async fn local_act_fail_and_retry(#[case] eventually_pass: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(50))),
                        backoff_coefficient: 1.2,
                        maximum_interval: None,
                        maximum_attempts: 5,
                        non_retryable_error_types: vec![],
                    },
                    ..Default::default()
                })
                .await;
            if eventually_pass {
                assert!(la_res.completed_ok())
            } else {
                assert!(la_res.failed())
            }
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
        if 2 == attempts.fetch_add(1, Ordering::Relaxed) && eventually_pass {
            Ok(())
        } else {
            Err(anyhow!("Oh no I failed!").into())
        }
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let expected_attempts = if eventually_pass { 3 } else { 5 };
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
}

#[tokio::test]
async fn local_act_retry_long_backoff_uses_timer() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_fail_marker(
        1,
        "1",
        Failure::application_failure("la failed".to_string(), false),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_fail_marker(
        2,
        "2",
        Failure::application_failure("la failed".to_string(), false),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), 2.into(), ResponseType::AllHistory],
        mock,
    );
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(65))),
                        // This will make the second backoff 65 seconds, plenty to use timer
                        backoff_coefficient: 1_000.,
                        maximum_interval: Some(prost_dur!(from_secs(600))),
                        maximum_attempts: 3,
                        non_retryable_error_types: vec![],
                    },
                    ..Default::default()
                })
                .await;
            assert!(la_res.failed());
            // Extra timer just to have an extra workflow task which we can return full history for
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        },
    );
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: String| async move {
            Result::<(), _>::Err(anyhow!("Oh no I failed!").into())
        },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_null_result() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, |_| {});
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "nullres".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("nullres", |_ctx: ActContext, _: String| async { Ok(()) });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_command_immediately_follows_la_marker() {
    // This repro only works both when cache is off, and there is at least one heartbeat wft
    // before the marker & next command are recorded.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    // Bug only repros when seeing history up to third wft
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [3], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 0);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "nullres".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        },
    );
    worker.register_activity("nullres", |_ctx: ActContext, _: String| async { Ok(()) });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn query_during_wft_heartbeat_doesnt_accidentally_fail_to_continue_heartbeat() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    // get query here
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_workflow_execution_completed();

    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(1));
        pr.queries = HashMap::new();
        pr.queries.insert(
            "the-query".to_string(),
            WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            },
        );
        pr
    };
    let after_la_resolved = Arc::new(Barrier::new(2));
    let poll_barr = after_la_resolved.clone();
    let tasks = [
        query_with_hist_task,
        hist_to_poll_resp(
            &t,
            wfid,
            ResponseType::UntilResolved(
                async move {
                    poll_barr.wait().await;
                }
                .boxed(),
                3,
            ),
        ),
    ];
    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let barrier = Barrier::new(2);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            schedule_local_activity_cmd(
                1,
                "1",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        // Get query, and complete it
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        // Now complete the LA
        barrier.wait().await;
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(&query.query_id, "whatev"),
        ))
        .await
        .unwrap();
        // Activation with it resolving:
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_execution(&task.run_id).await;
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        barrier.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        after_la_resolved.wait().await;
    };

    tokio::join!(wf_fut, act_fut);
}

#[rstest::rstest]
#[case::impossible_query_in_task(true)]
#[case::real_history(false)]
#[tokio::test]
async fn la_resolve_during_legacy_query_does_not_combine(#[case] impossible_query_in_task: bool) {
    // Ensures we do not send an activation with a legacy query and any other work, which should
    // never happen, but there was an issue where an LA resolving could trigger that.
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add(default_wes_attribs());
    // Since we don't send queries with start workflow, need one workflow task of something else
    // b/c we want to get an activation with a job and a nonlegacy query
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());

    // nonlegacy query got here & LA started here
    // then next task is incremental w/ legacy query (for impossible query case)
    t.add_full_wf_task();

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(2));
            // Strip beginning of history so the only events are WFT sched/started, we need to look
            // like we hit the cache
            {
                let h = pr.history.as_mut().unwrap();
                h.events = h.events.split_off(6);
            }
            // In the nonsense server response case, we attach a legacy query, otherwise this
            // response looks like a normal response to a forced WFT heartbeat.
            if impossible_query_in_task {
                pr.query = Some(WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                });
            }
            pr
        },
    ];
    let mut mock = mock_worker_client();
    if impossible_query_in_task {
        mock.expect_respond_legacy_query()
            .times(1)
            .returning(move |_, _| Ok(Default::default()));
    }
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let taskmap = mock.outstanding_task_map.clone().unwrap();
    let core = mock_worker(mock);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            },]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();

        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_)),
            },]
        );
        // We want to make sure the weird-looking query gets received while we're working on other
        // stuff, so that we don't see the workflow complete and choose to evict.
        taskmap.release_run(&task.run_id);
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            schedule_local_activity_cmd(
                1,
                "act-id",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();

        let task = core.poll_workflow_activation().await.unwrap();
        // The next task needs to be resolve, since the LA is completed immediately
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        // Complete workflow
        core.complete_execution(&task.run_id).await;

        // Now we will get the query
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
            }]
            if q.query_id == "q1"
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok("q1", "whatev"),
        ))
        .await
        .unwrap();

        if impossible_query_in_task {
            // finish last query
            let task = core.poll_workflow_activation().await.unwrap();
            core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                query_ok(LEGACY_QUERY_ID, "whatev"),
            ))
            .await
            .unwrap();
        }
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    };

    join!(wf_fut, act_fut);
    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn test_schedule_to_start_timeout() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::ToTaskNum(1)], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    // Impossibly small timeout so we timeout in the queue
                    schedule_to_start_timeout: prost_dur!(from_nanos(1)),
                    ..Default::default()
                })
                .await;
            assert_eq!(la_res.timed_out(), Some(TimeoutType::ScheduleToStart));
            let rfail = la_res.unwrap_failure();
            assert_matches!(
                rfail.failure_info,
                Some(FailureInfo::ActivityFailureInfo(_))
            );
            assert_matches!(
                rfail.cause.unwrap().failure_info,
                Some(FailureInfo::TimeoutFailureInfo(_))
            );
            Ok(().into())
        },
    );
    worker.register_activity(
        "echo",
        move |_ctx: ActContext, _: String| async move { Ok(()) },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::sched_to_start(true)]
#[case::sched_to_close(false)]
#[tokio::test]
async fn test_schedule_to_start_timeout_not_based_on_original_time(
    #[case] is_sched_to_start: bool,
) {
    // We used to carry over the schedule time of LAs from the "original" schedule time if these LAs
    // created newly after backing off across a timer. That was a mistake, since schedule-to-start
    // timeouts should apply to when the new attempt was scheduled. This test verifies:
    // * we don't time out on s-t-s timeouts because of that, when the param is true.
    // * we do properly time out on s-t-c timeouts when the param is false

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let orig_sched = SystemTime::now().sub(Duration::from_secs(60 * 20));
    t.add_local_activity_marker(
        1,
        "1",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |deets| {
            // Really old schedule time, which should _not_ count against schedule_to_start
            deets.original_schedule_time = Some(orig_sched.into());
            // Backoff value must be present since we're simulating timer backoff
            deets.backoff = Some(prost_dur!(from_secs(100)));
        },
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    let schedule_to_close_timeout = Some(if is_sched_to_start {
        // This 60 minute timeout will not have elapsed according to the original
        // schedule time in the history.
        Duration::from_secs(60 * 60)
    } else {
        // This 10 minute timeout will have already elapsed
        Duration::from_secs(10 * 60)
    });

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(50))),
                        backoff_coefficient: 1.2,
                        maximum_interval: None,
                        maximum_attempts: 5,
                        non_retryable_error_types: vec![],
                    },
                    schedule_to_start_timeout: Some(Duration::from_secs(60)),
                    schedule_to_close_timeout,
                    ..Default::default()
                })
                .await;
            if is_sched_to_start {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::ScheduleToClose));
            }
            Ok(().into())
        },
    );
    worker.register_activity(
        "echo",
        move |_ctx: ActContext, _: String| async move { Ok(()) },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn start_to_close_timeout_allows_retries(#[values(true, false)] la_completes: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    if la_completes {
        t.add_local_activity_marker(1, "1", Some("hi".into()), None, |_| {});
    } else {
        t.add_local_activity_marker(
            1,
            "1",
            None,
            Some(Failure::timeout(TimeoutType::StartToClose)),
            |_| {},
        );
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
    );
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(20))),
                        backoff_coefficient: 1.0,
                        maximum_interval: None,
                        maximum_attempts: 5,
                        non_retryable_error_types: vec![],
                    },
                    start_to_close_timeout: Some(prost_dur!(from_millis(25))),
                    ..Default::default()
                })
                .await;
            if la_completes {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::StartToClose));
            }
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    let cancels: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |ctx: ActContext, _: String| async move {
            // Timeout the first 4 attempts, or all of them if we intend to fail
            if attempts.fetch_add(1, Ordering::AcqRel) < 4 || !la_completes {
                select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                    _ = ctx.cancelled() => {
                        cancels.fetch_add(1, Ordering::AcqRel);
                        return Err(ActivityError::cancelled());
                    }
                }
            }
            Ok(())
        },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // Activity should have been attempted all 5 times
    assert_eq!(attempts.load(Ordering::Acquire), 5);
    let num_cancels = if la_completes { 4 } else { 5 };
    assert_eq!(cancels.load(Ordering::Acquire), num_cancels);
}

#[tokio::test]
async fn wft_failure_cancels_running_las() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2], mock);
    mh.num_expected_fails = 1;
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_handle = ctx.local_activity(LocalActivityOptions {
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            tokio::join!(
                async {
                    ctx.timer(Duration::from_secs(1)).await;
                    panic!("ahhh I'm failing wft")
                },
                la_handle
            );
            Ok(().into())
        },
    );
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |ctx: ActContext, _: String| async move {
            let res = tokio::time::timeout(Duration::from_millis(500), ctx.cancelled()).await;
            if res.is_err() {
                panic!("Activity must be cancelled!!!!");
            }
            Result::<(), _>::Err(ActivityError::cancelled())
        },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn resolved_las_not_recorded_if_wft_fails_many_times() {
    // We shouldn't record any LA results if the workflow activation is repeatedly failing. There
    // was an issue that, because we stop reporting WFT failures after 2 tries, this meant the WFT
    // was not marked as "completed" and the WFT could accidentally be replied to with LA results.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 2;
    mh.num_expected_completions = Some(0.into());
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    #[allow(unreachable_code)]
    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        WorkflowFunction::new::<_, _, ()>(|ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            panic!()
        }),
    );
    worker.register_activity(
        "echo",
        move |_: ActContext, _: String| async move { Ok(()) },
    );
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_records_nonfirst_attempts_ok() {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let nonfirst_counts = Arc::new(SegQueue::new());
    let nfc_c = nonfirst_counts.clone();
    mh.completion_mock_fn = Some(Box::new(move |c| {
        nfc_c.push(
            c.metering_metadata
                .nonfirst_local_activity_execution_attempts,
        );
        Ok(Default::default())
    }));
    let mut worker = mock_sdk_cfg(mh, |wc| {
        wc.max_cached_workflows = 1;
        wc.max_outstanding_workflow_tasks = Some(1);
    });

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    maximum_interval: None,
                    maximum_attempts: 0,
                    non_retryable_error_types: vec![],
                },
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        Result::<(), _>::Err(anyhow!("I fail").into())
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // 3 workflow tasks
    assert_eq!(nonfirst_counts.len(), 3);
    // First task's non-first count should, of course, be 0
    assert_eq!(nonfirst_counts.pop().unwrap(), 0);
    // Next two, some nonzero amount which could vary based on test load
    assert!(nonfirst_counts.pop().unwrap() > 0);
    assert!(nonfirst_counts.pop().unwrap() > 0);
}

#[tokio::test]
async fn local_activities_can_be_delivered_during_shutdown() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(
        wfid,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
        true,
    );
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    // Initiate shutdown once we have the WF activation, but before replying that we want to do an
    // LA
    core.initiate_shutdown();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleLocalActivity {
            seq: 1,
            activity_id: "1".to_string(),
            activity_type: "test_act".to_string(),
            start_to_close_timeout: Some(prost_dur!(from_secs(30))),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    let wf_poller = async { core.poll_workflow_activation().await };

    let at_poller = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        core.poll_activity_task().await
    };

    let (wf_r, act_r) = join!(wf_poller, at_poller);
    assert_matches!(wf_r.unwrap_err(), PollError::ShutDown);
    assert_matches!(act_r.unwrap_err(), PollError::ShutDown);
}

#[tokio::test]
async fn queries_can_be_received_while_heartbeating() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(3));
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            pr
        },
    ];
    let mut mock = mock_worker_client();
    mock.expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        schedule_local_activity_cmd(
            1,
            "act-id",
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
        ),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == "q1"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok("q1", "whatev"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == LEGACY_QUERY_ID
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID, "whatev"),
    ))
    .await
    .unwrap();

    // Handle the activity so we can shut down cleanly
    let act_task = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act_task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();

    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn local_activity_after_wf_complete_is_discarded() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        wfid,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
        mock,
    );
    mock_cfg.make_poll_stream_interminable = true;
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 0);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 2);
                assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
                assert_eq!(
                    wft.commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let barr = Barrier::new(2);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleLocalActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
            ScheduleLocalActivity {
                seq: 2,
                activity_id: "2".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    let wf_poller = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        barr.wait().await;
        core.complete_execution(&task.run_id).await;
    };

    let at_poller = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        let act_task = core.poll_activity_task().await.unwrap();
        barr.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![2].into())),
        })
        .await
        .unwrap();
    };

    join!(wf_poller, at_poller);
    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn local_act_retry_explicit_delay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(50))),
                        backoff_coefficient: 1.0,
                        maximum_attempts: 5,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await;
            assert!(la_res.completed_ok());
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
        let last_attempt = attempts.fetch_add(1, Ordering::Relaxed);
        if 0 == last_attempt {
            Err(ActivityError::Retryable {
                source: anyhow!("Explicit backoff error"),
                explicit_delay: Some(Duration::from_millis(300)),
            })
        } else if 2 == last_attempt {
            Ok(())
        } else {
            Err(anyhow!("Oh no I failed!").into())
        }
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let start = Instant::now();
    worker.run_until_done().await.unwrap();
    let expected_attempts = 3;
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
    // There will be one 300ms backoff and one 50s backoff, so things should take at least that long
    assert!(start.elapsed() > Duration::from_millis(350));
}
