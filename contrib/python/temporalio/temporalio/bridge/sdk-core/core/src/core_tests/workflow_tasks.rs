use crate::{
    PollWorkflowOptions, Worker, advance_fut,
    internal_flags::CoreInternalFlags,
    job_assert,
    replay::TestHistoryBuilder,
    test_help::{
        FakeWfResponses, MockPollCfg, MocksHolder, ResponseType, WorkerExt,
        WorkflowCachingPolicy::{self, AfterEveryReply, NonSticky},
        build_fake_worker, build_mock_pollers, build_multihist_mock_sg, canned_histories,
        gen_assert_and_fail, gen_assert_and_reply, hist_to_poll_resp, mock_sdk, mock_sdk_cfg,
        mock_worker, poll_and_reply, poll_and_reply_clears_outstanding_evicts, single_hist_mock_sg,
        test_worker_cfg,
    },
    worker::{
        TunerBuilder,
        client::mocks::{mock_manual_worker_client, mock_worker_client},
    },
};
use futures_util::{FutureExt, stream};
use mockall::TimesRange;
use rstest::{fixture, rstest};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::sync_channel,
    },
    time::Duration,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ActivityOptions, CancellableFuture, TimerOptions, WfContext};
use temporal_sdk_core_api::{
    Worker as WorkerTrait,
    errors::PollError,
    worker::{
        PollerBehavior, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext,
        SlotSupplier, SlotSupplierPermit, WorkerVersioningStrategy, WorkflowSlotKind,
    },
};
use temporal_sdk_core_protos::{
    DEFAULT_ACTIVITY_TYPE, DEFAULT_WORKFLOW_TYPE,
    coresdk::{
        activity_result::{self as ar, ActivityResolution, activity_resolution},
        common::VersioningIntent,
        workflow_activation::{
            FireTimer, InitializeWorkflow, ResolveActivity, UpdateRandomSeed,
            WorkflowActivationJob, remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, RequestCancelActivity,
            ScheduleActivity, SetPatchMarker, StartChildWorkflowExecution, UpdateResponse,
            update_response::Response, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    default_act_sched, default_wes_attribs,
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::{Payload, RetryPolicy, WorkerVersionStamp},
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::{
            TimerFiredEventAttributes, WorkflowPropertiesModifiedExternallyEventAttributes,
            history_event,
        },
        sdk::v1::UserMetadata,
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporal_sdk_core_test_utils::{WorkerTestHelpers, fanout_tasks, start_timer_cmd};
use tokio::{
    join,
    sync::{Barrier, Semaphore},
    time,
};

#[fixture(hist_batches = &[])]
fn single_timer_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_timer("1");
    build_fake_worker(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_activity("fake_activity");
    build_fake_worker(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_failure_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_failed_activity("fake_activity");
    build_fake_worker(wfid, t, hist_batches)
}

#[rstest]
#[case::incremental(single_timer_setup(&[1, 2]), NonSticky)]
#[case::replay(single_timer_setup(&[2]), NonSticky)]
#[case::incremental_evict(single_timer_setup(&[1, 2]), AfterEveryReply)]
#[case::replay_evict(single_timer_setup(&[2]), AfterEveryReply)]
#[tokio::test]
async fn single_timer(#[case] worker: Worker, #[case] evict: WorkflowCachingPolicy) {
    poll_and_reply(
        &worker,
        evict,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![start_timer_cmd(1, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(worker,
    case::incremental(single_activity_setup(&[1, 2])),
    case::incremental_activity_failure(single_activity_failure_setup(&[1, 2])),
    case::replay(single_activity_setup(&[2])),
    case::replay_activity_failure(single_activity_failure_setup(&[2]))
)]
#[tokio::test]
async fn single_activity_completion(worker: Worker) {
    poll_and_reply(
        &worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        activity_id: "fake_activity".to_string(),
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn parallel_timer_test_across_wf_bridge(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_1_id = 1;
    let timer_2_id = 2;

    let t = canned_histories::parallel_timer(
        timer_1_id.to_string().as_str(),
        timer_2_id.to_string().as_str(),
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    start_timer_cmd(timer_1_id, Duration::from_secs(1)),
                    start_timer_cmd(timer_2_id, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [
                            WorkflowActivationJob {
                                variant: Some(workflow_activation_job::Variant::FireTimer(
                                    FireTimer { seq: t1_id }
                                )),
                            },
                            WorkflowActivationJob {
                                variant: Some(workflow_activation_job::Variant::FireTimer(
                                    FireTimer { seq: t2_id }
                                )),
                            }
                        ] => {
                            assert_eq!(t1_id, &timer_1_id);
                            assert_eq!(t2_id, &timer_2_id);
                        }
                    );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn timer_cancel(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_id = 1;
    let cancel_timer_id = 2;

    let t = canned_histories::cancel_timer(
        timer_id.to_string().as_str(),
        cancel_timer_id.to_string().as_str(),
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    start_timer_cmd(cancel_timer_id, Duration::from_secs(1)),
                    start_timer_cmd(timer_id, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![
                    CancelTimer {
                        seq: cancel_timer_id,
                    }
                    .into(),
                    CompleteWorkflowExecution { result: None }.into(),
                ],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_seq = 1;
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_seq,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity is getting resolved right away as we are in the TryCancel mode.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_seq = 1;
    let activity_id = "fake_activity";

    let t = canned_histories::scheduled_activity_timeout(activity_id);
    let core = build_fake_worker(wfid, t, hist_batches);
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_id.to_string(),
                    ..default_act_sched()
                }
                    .into()],
            ),
            // Activity is getting resolved right away as it has been timed out.
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                    res.jobs.as_slice(),
                    [
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                                ResolveActivity {
                                    seq,
                                    result: Some(ActivityResolution {
                                        status: Some(activity_resolution::Status::Failed(ar::Failure {
                                            failure: Some(failure)
                                        })),
                                    }), ..
                                }
                            )),
                        }
                    ] => {
                        assert_eq!(failure.message, "Activity task timed out".to_string());
                        assert_eq!(*seq, activity_seq);
                    }
                );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
        .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_seq = 1;

    let t = canned_histories::started_activity_timeout(activity_seq.to_string().as_str());
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_seq.to_string(),
                    ..default_act_sched()
                }
                    .into()],
            ),
            // Activity is getting resolved right away as it has been timed out.
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                    res.jobs.as_slice(),
                    [
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                                ResolveActivity {
                                    seq,
                                    result: Some(ActivityResolution {
                                        status: Some(activity_resolution::Status::Failed(ar::Failure {
                                            failure: Some(failure)
                                        })),
                                    }), ..
                                }
                            )),
                        }
                    ] => {
                        assert_eq!(failure.message, "Activity task timed out".to_string());
                        assert_eq!(*seq, activity_seq);
                    }
                );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
        .await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn cancelled_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_seq = 0;
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_seq,
                        activity_id: activity_id.to_string(),
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity is resolved right away as it has timed out.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_cancellation_abandon(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_abandon(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_cancellation_abandon(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_abandon(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn abandoned_activities_ignore_start_and_complete(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let activity_id = "1";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    let act_started_event_id = t.add_activity_task_started(act_scheduled_event_id);
    t.add_activity_task_completed(
        act_scheduled_event_id,
        act_started_event_id,
        Default::default(),
    );
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let mock = mock_worker_client();
    let mut worker = mock_sdk(MockPollCfg::from_resp_batches(wfid, t, hist_batches, mock));

    worker.register_wf(wf_type.to_owned(), |ctx: WfContext| async move {
        let act_fut = ctx.activity(ActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            cancellation_type: ActivityCancellationType::Abandon,
            ..Default::default()
        });
        ctx.timer(Duration::from_secs(1)).await;
        act_fut.cancel(&ctx);
        ctx.timer(Duration::from_secs(3)).await;
        act_fut.await;
        Ok(().into())
    });
    worker
        .submit_wf(wfid, wf_type, vec![], Default::default())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_with_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

/// Verification for try cancel & abandon histories
async fn verify_activity_cancellation(
    worker: &Worker,
    activity_seq: u32,
    cancel_type: ActivityCancellationType,
) {
    poll_and_reply(
        worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_seq,
                        activity_id: activity_seq.to_string(),
                        cancellation_type: cancel_type as i32,
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity should be resolved right away
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn scheduled_activity_cancellation_wait_for_cancellation(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_signal_and_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_with_signal_and_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

async fn verify_activity_cancellation_wait_for_cancellation(activity_id: u32, worker: &Worker) {
    poll_and_reply(
        worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::WaitCancellationCompleted
                            as i32,
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_id }.into()],
            ),
            // Making sure that activity is not resolved until it's cancelled.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![],
            ),
            // Now ActivityTaskCanceled has been processed and activity can be resolved.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn workflow_update_random_seed_on_workflow_reset() {
    let wfid = "fake_wf_id";
    let new_run_id = "86E39A5F-AE31-4626-BDFE-398EE072D156";
    let timer_1_id = 1;
    let randomness_seed_from_start = AtomicU64::new(0);

    let t = canned_histories::workflow_fails_with_reset_after_timer(
        timer_1_id.to_string().as_str(),
        new_run_id,
    );
    let core = build_fake_worker(wfid, t, [2]);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(
                            InitializeWorkflow{randomness_seed, ..}
                            )),
                        }] => {
                        randomness_seed_from_start.store(*randomness_seed, Ordering::SeqCst);
                        }
                    );
                },
                vec![start_timer_cmd(timer_1_id, Duration::from_secs(1))],
            ),
            // The random seed update should always be the first job
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::UpdateRandomSeed(
                                UpdateRandomSeed{randomness_seed})),
                        },
                            WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::FireTimer(_),),
                        },
                        ] => {
                            assert_ne!(randomness_seed_from_start.load(Ordering::SeqCst),
                                      *randomness_seed);
                        }
                    );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn cancel_timer_before_sent_wf_bridge() {
    let wfid = "fake_wf_id";
    let cancel_timer_id = 1;

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_worker(wfid, t, [1]);

    poll_and_reply(
        &core,
        NonSticky,
        &[gen_assert_and_reply(
            &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
            vec![
                start_timer_cmd(cancel_timer_id, Duration::from_secs(1)),
                CancelTimer {
                    seq: cancel_timer_id,
                }
                .into(),
                CompleteWorkflowExecution { result: None }.into(),
            ],
        )],
    )
    .await;
}

#[rstest]
#[case::no_evict_inc(&[1, 2, 2], NonSticky)]
#[case::no_evict(&[2, 2], NonSticky)]
#[tokio::test]
async fn complete_activation_with_failure(
    #[case] batches: &'static [usize],
    #[case] evict: WorkflowCachingPolicy,
) {
    let wfid = "fake_wf_id";
    let timer_id = 1;

    let hist =
        canned_histories::workflow_fails_with_failure_after_timer(timer_id.to_string().as_str());
    let mock_sg = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: batches.iter().map(Into::into).collect(),
        }],
        true,
        1,
    );
    let core = mock_worker(mock_sg);

    poll_and_reply(
        &core,
        evict,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![start_timer_cmd(timer_id, Duration::from_secs(1))],
            ),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
    core.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn simple_timer_fail_wf_execution(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_id = 1;

    let t = canned_histories::single_timer(timer_id.to_string().as_str());
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![start_timer_cmd(timer_id, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![
                    FailWorkflowExecution {
                        failure: Some(Failure {
                            message: "I'm ded".to_string(),
                            ..Default::default()
                        }),
                    }
                    .into(),
                ],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn two_signals(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";

    let t = canned_histories::two_signals("sig1", "sig2");
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                // Task is completed with no commands
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::SignalWorkflow(_)
                ),
                vec![],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn workflow_failures_only_reported_once() {
    let wfid = "fake_wf_id";
    let timer_1 = 1;
    let timer_2 = 2;

    let hist = canned_histories::workflow_fails_with_failure_two_different_points(
        timer_1.to_string().as_str(),
        timer_2.to_string().as_str(),
    );
    let response_batches = vec![
        1, 2, // Start then first good reply
        2, 2, 2, // Poll for every failure
        // Poll again after evicting after second good reply, then two more fails
        3, 3, 3,
    ];
    let mocks = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: response_batches.into_iter().map(Into::into).collect(),
        }],
        true,
        // We should only call the server to say we failed twice (once after each success)
        2,
    );
    let omap = mocks.outstanding_task_map.clone();
    let core = mock_worker(mocks);

    poll_and_reply_clears_outstanding_evicts(
        &core,
        omap,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![start_timer_cmd(timer_1, Duration::from_secs(1))],
            ),
            // Fail a few times in a row (only one of which should be reported)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![start_timer_cmd(timer_2, Duration::from_secs(1))],
            ),
            // Again (a new fail should be reported here)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn max_wft_respected() {
    let total_wfs = 100;
    let wf_ids: Vec<_> = (0..total_wfs).map(|i| format!("fake-wf-{i}")).collect();
    let hists = wf_ids.iter().map(|wf_id| {
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id: wf_id.to_string(),
            hist,
            response_batches: vec![1.into(), 2.into()],
        }
    });
    let mh = MockPollCfg::new(hists.into_iter().collect(), true, 0);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        cfg.max_cached_workflows = total_wfs as usize;
        cfg.max_outstanding_workflow_tasks = Some(1);
    });
    let active_count: &'static _ = Box::leak(Box::new(Semaphore::new(1)));
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        drop(
            active_count
                .try_acquire()
                .expect("No multiple concurrent workflow tasks!"),
        );
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    for wf_id in wf_ids {
        worker
            .submit_wf(wf_id, DEFAULT_WORKFLOW_TYPE, vec![], Default::default())
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_on_replay_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::unsent_at_cancel_repro();
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                // Start timer and activity
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..default_act_sched()
                    }
                    .into(),
                    start_timer_cmd(1, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![RequestCancelActivity { seq: activity_id }.into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![start_timer_cmd(2, Duration::from_secs(1))],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_when_also_completed_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::cancel_not_sent_when_also_complete_repro();
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: "act-1".to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity { seq: activity_id }.into(),
                    start_timer_cmd(2, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn lots_of_workflows() {
    let total_wfs = 500;
    let hists = (0..total_wfs).map(|i| {
        let wf_id = format!("fake-wf-{i}");
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1.into(), 2.into()],
        }
    });
    let mut mock = build_multihist_mock_sg(hists, false, 0);
    mock.make_wft_stream_interminable();
    let worker = &mock_worker(mock);
    let completed_count = Arc::new(Semaphore::new(0));
    let killer = async {
        let _ = completed_count.acquire_many(total_wfs).await.unwrap();
        worker.initiate_shutdown();
    };
    let poller = fanout_tasks(5, |_| {
        let completed_count = completed_count.clone();
        async move {
            while let Ok(wft) = worker.poll_workflow_activation().await {
                let job = &wft.jobs[0];
                let reply = match job.variant {
                    Some(workflow_activation_job::Variant::InitializeWorkflow(_)) => {
                        start_timer_cmd(1, Duration::from_secs(1))
                    }
                    Some(workflow_activation_job::Variant::RemoveFromCache(_)) => {
                        worker
                            .complete_workflow_activation(WorkflowActivationCompletion::empty(
                                wft.run_id,
                            ))
                            .await
                            .unwrap();
                        continue;
                    }
                    _ => {
                        completed_count.add_permits(1);
                        CompleteWorkflowExecution { result: None }.into()
                    }
                };
                worker
                    .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                        wft.run_id, reply,
                    ))
                    .await
                    .unwrap();
            }
        }
    });
    join!(killer, poller);
    worker.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn wft_timeout_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::wft_timeout_repro();
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..default_act_sched()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::ResolveActivity(ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Completed(..)),
                        }),
                        ..
                    })
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn complete_after_eviction() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task().times(0);
    let mock = single_hist_mock_sg(wfid, t, [2], mock, true);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    // We just got start workflow, immediately evict
    core.request_workflow_eviction(&activation.run_id);
    // Since we got whole history, we must finish replay before eviction will appear
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let next_activation = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        next_activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        next_activation.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn sends_appropriate_sticky_task_queue_responses() {
    // This test verifies that when completions are sent with sticky queues enabled, that they
    // include the information that tells the server to enqueue the next task on a sticky queue.
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .withf(|comp| comp.sticky_attributes.is_some())
        .times(1)
        .returning(|_| Ok(Default::default()));
    mock.expect_complete_workflow_task().times(0);
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, false);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn new_server_work_while_eviction_outstanding_doesnt_overwrite_activation() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let mock = single_hist_mock_sg(wfid, t, [1, 2], mock_worker_client(), false);
    let taskmap = mock.outstanding_task_map.clone().unwrap();
    let core = mock_worker(mock);

    // Poll for and complete first workflow task
    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let evict_act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        evict_act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Ensure mock has delivered both tasks
    assert!(taskmap.all_work_delivered());
    // Now we can complete the evict
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(evict_act.run_id))
        .await
        .unwrap();
    // The task buffered during eviction is applied and we start over
    let start_again = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        start_again.jobs[0].variant,
        Some(workflow_activation_job::Variant::InitializeWorkflow(_))
    );
}

#[tokio::test]
async fn buffered_work_drained_on_shutdown() {
    let wfid = "fake_wf_id";
    // Build a one-timer history where first task times out
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    // Need to build the first response before adding the timeout events b/c otherwise the history
    // builder will include them in the first task
    let resp_1 = hist_to_poll_resp(&t, wfid.to_owned(), 1.into()).resp;
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add(history_event::Attributes::TimerFiredEventAttributes(
        TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "1".to_string(),
        },
    ));
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut tasks = VecDeque::from(vec![resp_1]);
    // Extend the task list with the now timeout-included version of the task. We add a bunch of
    // them because the poll loop will spin while new tasks are available and it is buffering them
    tasks.extend(
        std::iter::repeat_with(|| hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp).take(50),
    );
    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let mut mock = MocksHolder::from_wft_stream(mock, stream::iter(tasks));
    // Cache on to avoid being super repetitive
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = &mock_worker(mock);

    // Poll for first WFT
    let act1 = core.poll_workflow_activation().await.unwrap();
    let poll_fut = async move {
        // Now poll again, which will start spinning, and buffer the next WFT with timer fired in it
        // - it won't stop spinning until the first task is complete
        let t = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            t.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    };
    let complete_first = async move {
        // If the first complete is sent too fast, we may not have had a chance to buffer work.
        tokio::time::sleep(Duration::from_millis(50)).await;
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            act1.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    join!(poll_fut, complete_first, async {
        core.shutdown().await;
    });
}

#[tokio::test]
async fn fail_wft_then_recover() {
    let t = canned_histories::long_sequential_timers(1);
    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        // We need to deliver all of history twice because of eviction
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock_worker_client(),
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
    });
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    // Start an activity instead of a timer, triggering nondeterminism error
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id.clone(),
        vec![
            ScheduleActivity {
                activity_id: "fake_activity".to_string(),
                ..default_act_sched()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    // We must handle an eviction now
    core.handle_eviction().await;

    // Workflow starting over, this time issue the right command
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn poll_response_triggers_wf_error() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    // Add this nonsense event here to make applying the poll response fail
    t.add_external_signal_completed(100);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory],
        mock_worker_client(),
    );
    // Fail wft will be called when auto-failing.
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(move |_, cause, _| {
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
    });
    let mock = build_mock_pollers(mh);
    let core = mock_worker(mock);
    // Poll for first WFT, which is immediately an eviction
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
}

// Verifies we can handle multiple wft timeouts in a row if lang is being very slow in responding
#[tokio::test]
async fn lang_slower_than_wft_timeouts() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::not_found("Workflow task not found.")));
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, [1, 1], mock, true);
    let tasksmap = mock.outstanding_task_map.clone().unwrap();
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
    });
    let core = mock_worker(mock);

    // This completion runs into the workflow task not found error
    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // It will get an eviction
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Before we complete, unlock the next task from the mock so that we'll see it get buffered.
    tasksmap.release_run(&wf_task.run_id);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // The buffered WFT should be applied now
    let start_again = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        start_again.jobs[0].variant,
        Some(workflow_activation_job::Variant::InitializeWorkflow(_))
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        start_again.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn tries_cancel_of_completed_activity() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("1");
    t.add_we_signaled("sig", vec![]);
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, Default::default());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg("fake_wf_id", t, [1, 2], mock, true);
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        ScheduleActivity {
            seq: 1,
            activity_id: "1".to_string(),
            ..default_act_sched()
        }
        .into(),
    ))
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        activation.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        activation.run_id,
        vec![
            RequestCancelActivity { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn failing_wft_doesnt_eat_permit_forever() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock = MockPollCfg::from_resp_batches("fake_wf_id", t, [1, 1, 1], mock);
    mock.num_expected_fails = 1;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 2;
        cfg.max_outstanding_workflow_tasks = Some(2);
    });
    let outstanding_mock_tasks = mock.outstanding_task_map.clone();
    let worker = mock_worker(mock);

    let mut run_id = "".to_string();
    // Fail twice, verifying a permit is not eaten. We cannot fail the same run more than twice in a
    // row because we purposefully time out rather than spamming.
    for _ in 1..=2 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        run_id.clone_from(&activation.run_id);
        // Issue a nonsense completion that will trigger a WFT failure
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                activation.run_id,
                RequestCancelActivity { seq: 1 }.into(),
            ))
            .await
            .unwrap();
        worker.handle_eviction().await;
    }
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
    // We should be "out of work" because the mock service thinks we didn't complete the last task,
    // which we didn't, because we don't spam failures. The real server would eventually time out
    // the task. Mock doesn't understand that, so the WFT permit is released because eventually a
    // new one will be generated. We manually clear the mock's outstanding task list so the next
    // poll will work.
    outstanding_mock_tasks.unwrap().release_run(&run_id);
    let activation = worker.poll_workflow_activation().await.unwrap();
    // There should be no change in permits, since this just unbuffered the buffered task
    assert_eq!(worker.available_wft_permits(), Some(1));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();
    worker.shutdown().await;
    assert_eq!(worker.available_wft_permits(), Some(2));
}

#[tokio::test]
async fn cache_miss_will_fetch_history() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let get_exec_resp: GetWorkflowExecutionHistoryResponse = t.get_history_info(2).unwrap().into();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::ToTaskNum(1), ResponseType::OneTask(2)],
        mock_worker_client(),
    );
    mh.mock_client
        .expect_get_workflow_execution_history()
        .times(1)
        .returning(move |_, _, _| Ok(get_exec_resp.clone()));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 1;
        // Also verifies tying the WFT permit to the fetch request doesn't get us stuck
        cfg.max_outstanding_workflow_tasks = Some(1);
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 3);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    // Force an eviction (before complete matters, so that we will be sure the eviction is queued
    // up before the next fake WFT is unlocked)
    worker.request_wf_eviction(
        &activation.run_id,
        "whatever",
        EvictionReason::LangRequested,
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(&activation.run_id))
        .await
        .unwrap();
    // Handle the eviction, and the restart
    for i in 1..=2 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(activation.history_length, 3);
        if i == 1 {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                }]
            );
        } else {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
                }]
            );
        }
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    }
    let activation = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 7);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
    worker.shutdown().await;
}

#[tokio::test]
async fn history_byte_size_and_can_suggestion_in_activation() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t.modify_event(7, |he| {
        if let Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(ref mut attrs)) =
            he.attributes
        {
            attrs.suggest_continue_as_new = true;
        }
    });

    let mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::ToTaskNum(1), ResponseType::OneTask(2)],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    // Test builder always does num events * 10
    assert_eq!(activation.history_size_bytes, 30);
    assert!(!activation.continue_as_new_suggested);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
        .await
        .unwrap();
    let activation = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_size_bytes, 70);
    assert!(activation.continue_as_new_suggested);
}

/// This test verifies that WFTs which come as replies to completing a WFT are properly delivered
/// via activation polling.
#[tokio::test]
async fn tasks_from_completion_are_delivered() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_worker_client();
    let complete_resp = RespondWorkflowTaskCompletedResponse {
        workflow_task: Some(hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp),
        activity_tasks: vec![],
        reset_history_event_id: 0,
    };
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |_| Ok(complete_resp.clone()));
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);

    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn pagination_works_with_tasks_from_completion() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_workflow_task_scheduled_and_started();

    let mut mock = mock_worker_client();
    let mut needs_pag_resp = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2)).resp;
    needs_pag_resp.next_page_token = vec![1];
    let complete_resp = RespondWorkflowTaskCompletedResponse {
        workflow_task: Some(needs_pag_resp),
        ..Default::default()
    };
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |_| Ok(complete_resp.clone()));
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Ok(Default::default()));

    let get_exec_resp: GetWorkflowExecutionHistoryResponse =
        t.get_full_history_info().unwrap().into();
    mock.expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(get_exec_resp.clone()))
        .times(1);

    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);

    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn poll_faster_than_complete_wont_overflow_cache() {
    // Make workflow tasks for 5 different runs
    let tasks: Vec<_> = (1..=5)
        .map(|i| FakeWfResponses {
            wf_id: format!("wf-{i}"),
            hist: canned_histories::single_timer("1"),
            response_batches: vec![ResponseType::ToTaskNum(1)],
        })
        .collect();
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(3)
        .returning(|_| Ok(Default::default()));
    let mut mock_cfg = MockPollCfg::new(tasks, true, 0);
    mock_cfg.mock_client = mock_client;
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 3;
        wc.max_outstanding_workflow_tasks = Some(3);
    });
    let core = mock_worker(mock);
    // Poll 4 times, completing once, such that max tasks are never exceeded
    let p1 = core.poll_workflow_activation().await.unwrap();
    let p2 = core.poll_workflow_activation().await.unwrap();
    let p3 = core.poll_workflow_activation().await.unwrap();
    for (i, p_res) in [&p1, &p2, &p3].into_iter().enumerate() {
        assert_matches!(
            &p_res.jobs[0].variant,
            Some(workflow_activation_job::Variant::InitializeWorkflow(sw))
            if sw.workflow_id == format!("wf-{}", i + 1)
        );
    }
    // Complete first task to free a wft slot. Cache size is at 3
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        p1.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    // Now we're at cache limit. We will poll for a task, discover it is for a new run, issue
    // an eviction, and buffer the new run task. However, the run we're trying to evict has pending
    // activations! Thus, we must complete them first before this poll will unblock, and then it
    // will unblock with the eviciton.
    let p4 = core.poll_workflow_activation();
    // Make sure the task gets buffered before we start the complete, so the LRU list is in the
    // expected order and what we expect to evict will be evicted.
    advance_fut!(p4);
    let p4 = async {
        let p4 = p4.await.unwrap();
        assert_matches!(
            &p4.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        p4
    };
    let p2_pending_completer = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            p2.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    let (p4, _) = join!(p4, p2_pending_completer);
    assert_eq!(core.cached_workflows().await, 3);

    // This poll should also block until the eviction is actually completed
    let blocking_poll = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            &res.jobs[0].variant,
            Some(workflow_activation_job::Variant::InitializeWorkflow(sw))
            if sw.workflow_id == format!("wf-{}", 4)
        );
        res
    };
    let complete_evict = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(p4.run_id))
            .await
            .unwrap();
    };

    let (_p5, _) = join!(blocking_poll, complete_evict);
    assert_eq!(core.cached_workflows().await, 3);
    // The next poll will get an buffer a task for a new run, and generate an eviction for p3 but
    // that eviction cannot be obtained until we complete the existing outstanding task.
    let p6 = async {
        let p6 = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            p6.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        p6
    };
    let completer = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            p3.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    let (p6, _) = join!(p6, completer);
    let complete_evict = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(p6.run_id))
            .await
            .unwrap();
    };
    let blocking_poll = async {
        // This poll will also block until the last eviction goes through, and when it does it'll
        // produce the final start workflow task
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            &res.jobs[0].variant,
            Some(workflow_activation_job::Variant::InitializeWorkflow(sw))
            if sw.workflow_id == "wf-5"
        );
    };

    join!(blocking_poll, complete_evict);
    // p5 outstanding and final poll outstanding -- hence one permit available
    assert_eq!(core.available_wft_permits(), Some(1));
    assert_eq!(core.cached_workflows().await, 3);
}

#[tokio::test]
async fn eviction_waits_until_replay_finished() {
    let wfid = "fake_wf_id";
    let t = canned_histories::long_sequential_timers(3);
    let mock = mock_worker_client();
    let mock = single_hist_mock_sg(wfid, t, [3], mock, true);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 3);
    // Immediately request eviction after getting start workflow
    core.request_workflow_eviction(&activation.run_id);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let t1_fired = core.poll_workflow_activation().await.unwrap();
    assert_eq!(t1_fired.history_length, 8);
    assert_matches!(
        t1_fired.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        t1_fired.run_id,
        start_timer_cmd(2, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let t2_fired = core.poll_workflow_activation().await.unwrap();
    assert_eq!(t2_fired.history_length, 13);
    assert_matches!(
        t2_fired.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        t2_fired.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn autocompletes_wft_no_work() {
    let wfid = "fake_wf_id";
    let activity_id = "1";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_full_wf_task();
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task();
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, Default::default());
    t.add_full_wf_task();
    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(wfid, t, [1, 2, 3, 4], mock, true);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        ScheduleActivity {
            seq: 1,
            activity_id: activity_id.to_string(),
            cancellation_type: ActivityCancellationType::Abandon as i32,
            ..default_act_sched()
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        RequestCancelActivity { seq: 1 }.into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    // The last task will autocomplete, and thus this will return shutdown since there is no more
    // work
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );

    core.shutdown().await;
}

#[tokio::test]
async fn no_race_acquiring_permits() {
    let wfid = "fake_wf_id";
    let mut mock_client = mock_manual_worker_client();
    // We need to allow two polls to happen by triggering two processing events in the workflow
    // stream, but then delivering the actual tasks after that
    let task_barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| {
            let t = canned_histories::single_timer("1");
            let poll_resp = hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp;
            async move {
                task_barr.wait().await;
                Ok(poll_resp.clone())
            }
            .boxed()
        });
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| async move { Ok(Default::default()) }.boxed());

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_outstanding_workflow_tasks(2_usize)
            .max_cached_workflows(0_usize)
            .build()
            .unwrap(),
        mock_client,
    );

    // Two polls in a row, both of which will get stuck on the barrier and are only allowed to
    // proceed after a call which will cause the workflow stream to process an event. Without the
    // fix, this would've meant the stream though it was OK to poll twice, but once the tasks
    // are received, it would find there was only one permit.
    let poll_1_f = async {
        let r = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                r.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let poll_2_f = async {
        let r = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                r.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let other_f = async {
        worker.cached_workflows().await;
        task_barr.wait().await;
        worker.cached_workflows().await;
        task_barr.wait().await;
    };
    join!(poll_1_f, poll_2_f, other_f);
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn continue_as_new_preserves_some_values() {
    let wfid = "fake_wf_id";
    let memo = HashMap::<String, Payload>::from([("enchi".to_string(), b"cat".into())]).into();
    let search = HashMap::<String, Payload>::from([("noisy".to_string(), b"kitty".into())]).into();
    let retry_policy = RetryPolicy {
        backoff_coefficient: 13.37,
        ..Default::default()
    };
    let mut wes_attrs = default_wes_attribs();
    wes_attrs.memo = Some(memo);
    wes_attrs.search_attributes = Some(search);
    wes_attrs.retry_policy = Some(retry_policy);
    let mut mock_client = mock_worker_client();
    let t = {
        let mut t = TestHistoryBuilder::default();
        t.add(wes_attrs.clone());
        t.add_full_wf_task();
        t
    };
    mock_client
        .expect_complete_workflow_task()
        .returning(move |mut c| {
            let cmd = c.commands.pop().unwrap().attributes.unwrap();
            if let Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(a) = cmd {
                assert_eq!(a.workflow_type.unwrap().name, "meow");
                assert_eq!(a.memo, wes_attrs.memo);
                assert_eq!(a.search_attributes, wes_attrs.search_attributes);
                assert_eq!(a.retry_policy, wes_attrs.retry_policy);
            } else {
                panic!("Wrong attributes type");
            }
            Ok(Default::default())
        });
    let mock = single_hist_mock_sg(wfid, t, vec![ResponseType::AllHistory], mock_client, true);
    let worker = mock_worker(mock);
    let r = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            r.run_id,
            ContinueAsNewWorkflowExecution {
                workflow_type: "meow".to_string(),
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();
    worker.shutdown().await;
}

#[rstest]
#[tokio::test]
async fn ignorable_events_are_ok(#[values(true, false)] attribs_unset: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let id = t.add(WorkflowPropertiesModifiedExternallyEventAttributes::default());
    t.modify_event(id, |e| {
        e.worker_may_ignore = true;
        // Ignorable events are ignored if we can't interpret the proto of either the event attribs
        // or proto - otherwise (this is the _may_ part of may ignore), we'll still try to process
        // it. That processing may ultimately still choose to do nothing, if we want to _explicitly_
        // ignore it.
        if attribs_unset {
            e.attributes = None;
        } else {
            e.event_type = EventType::Unspecified as i32;
        }
    });
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mock = single_hist_mock_sg("wheee", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant,
        Some(workflow_activation_job::Variant::InitializeWorkflow(_))
    );
}

#[tokio::test]
async fn fetching_to_continue_replay_works() {
    let mut mock_client = mock_worker_client();
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task(); // ends 7
    let mut need_fetch_resp =
        hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
    need_fetch_resp.next_page_token = vec![1];

    t.add_full_wf_task();
    t.add_we_signaled("hi", vec![]); // Need to make there be two complete WFTs
    t.add_full_wf_task(); // end 14
    let mut fetch_resp: GetWorkflowExecutionHistoryResponse =
        t.get_full_history_info().unwrap().into();
    // And indicate that even *more* needs to be fetched after this, so we see a request for the
    // next page happen.
    fetch_resp.next_page_token = vec![2];

    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    let final_fetch_resp: GetWorkflowExecutionHistoryResponse =
        t.get_full_history_info().unwrap().into();

    let tasks = vec![
        ResponseType::ToTaskNum(1),
        ResponseType::Raw(need_fetch_resp),
    ];
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(fetch_resp.clone()))
        .times(1);
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Ok(final_fetch_resp.clone()))
        .times(1);
    let mut mock = single_hist_mock_sg("wfid", t, tasks, mock_client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant,
        Some(workflow_activation_job::Variant::InitializeWorkflow(_))
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
}

#[tokio::test]
async fn fetching_error_evicts_wf() {
    let mut mock_client = mock_worker_client();
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed();
    let mut need_fetch_resp =
        hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
    need_fetch_resp.next_page_token = vec![1];
    let tasks = vec![
        ResponseType::ToTaskNum(1),
        ResponseType::Raw(need_fetch_resp),
    ];
    mock_client
        .expect_get_workflow_execution_history()
        .returning(move |_, _, _| Err(tonic::Status::not_found("Ahh broken")))
        .times(1);
    let mut mock = single_hist_mock_sg("wfid", t, tasks, mock_client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant,
        Some(workflow_activation_job::Variant::InitializeWorkflow(_))
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    let evict_act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        evict_act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(r)),
        }] => r.message.contains("Fetching history failed")
    );
}

/// This test verifies that if we fail to fetch a page during a completion, that we don't get stuck
/// in the complete waiting for the completion to finish.
#[tokio::test]
async fn ensure_fetching_fail_during_complete_sends_task_failure() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task(); // started 3
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task(); // started 7

    // Need a command event after here so the paginator will know it has two complete WFTs and
    // processing can begin before needing to fetch again
    t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task(); // started 11
    t.add_workflow_execution_completed();

    let mut first_poll = hist_to_poll_resp(&t, wfid, ResponseType::OneTask(4)).resp;
    // History is partial so fetch will happen. We have to lie here and make up a previous started
    // which really makes no sense, otherwise the paginator eagerly fetches and will fail before we
    // ever start anything -- which is good -- but this test wants to make sure a fetching failure
    // during a completion is handled correctly. That may no longer actually be a thing that can
    // happen.
    first_poll.previous_started_event_id = 0;
    first_poll.started_event_id = 11;

    let mut next_page: GetWorkflowExecutionHistoryResponse =
        t.get_full_history_info().unwrap().into();
    next_page.history.as_mut().unwrap().events.truncate(9);
    next_page.next_page_token = vec![2];

    let mut mock = mock_worker_client();
    mock.expect_get_workflow_execution_history()
        .returning(move |_, _, _| {
            error!("Called fetch!");
            Ok(next_page.clone())
        })
        .times(1);
    mock.expect_get_workflow_execution_history()
        .returning(move |_, _, _| {
            error!("Called fetch second time!");
            Err(tonic::Status::not_found("Ahh broken"))
        })
        .times(1);
    mock.expect_fail_workflow_task()
        .returning(|_, _, _| Ok(Default::default()))
        .times(1);

    let mut mock = single_hist_mock_sg(wfid, t, [ResponseType::Raw(first_poll)], mock, true);
    mock.make_wft_stream_interminable();
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);

    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();

    // Expect to see eviction b/c of history fetching error here.
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(c)),
        }] if c.message.contains("Fetching history")
    );

    core.shutdown().await;
}

#[tokio::test]
async fn lang_internal_flags() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(&[], &[1]);
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task();
    t.set_flags_last_wft(&[], &[2]);
    t.add_we_signaled("sig2", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::ToTaskNum(2), ResponseType::AllHistory],
        mock_worker_client(),
    );
    mh.completion_mock_fn = Some(Box::new(|c| {
        assert_matches!(c.sdk_metadata.lang_used_flags.as_slice(), &[2]);
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(act.available_internal_flags.as_slice(), [1]);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    let mut completion = WorkflowActivationCompletion::empty(act.run_id);
    completion.add_internal_flags(2);
    core.complete_workflow_activation(completion).await.unwrap();

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(act.available_internal_flags.as_slice(), [1, 2]);
    core.complete_execution(&act.run_id).await;
    core.shutdown().await;
}

#[tokio::test]
async fn lang_internal_flag_with_update() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_last_wft(&[1, 2], &[1]);
    let updid = t.add_update_accepted("upd1", "upd");
    t.add_update_completed(updid);
    t.add_workflow_execution_completed();

    let mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            },
        ]
    );
    assert_matches!(act.available_internal_flags.as_slice(), [1]);
    let mut completion = WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Accepted(())),
            }
            .into(),
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Default::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    );
    completion.add_internal_flags(1);
    core.complete_workflow_activation(completion).await.unwrap();
}

// Verify we send all core internal flags on the first non-replay WFT
#[tokio::test]
async fn core_internal_flags() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::ToTaskNum(1)],
        mock_worker_client(),
    );
    mh.completion_mock_fn = Some(Box::new(move |c| {
        assert_eq!(
            c.sdk_metadata
                .core_used_flags
                .iter()
                .copied()
                .collect::<HashSet<_>>(),
            CoreInternalFlags::all_except_too_high()
                .map(|f| f as u32)
                .collect()
        );
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&act.run_id).await;
    core.shutdown().await;
}

#[tokio::test]
async fn post_terminal_commands_are_retained_when_not_replaying() {
    // History contains a non-terminal command (N) followed by the terminal
    // command (T). The test establishes that, when lang completes an activation
    // with [T, N], core emits commands [N, T].
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_workflow_execution_completed();

    let commands_sent_by_lang = vec![
        CompleteWorkflowExecution { result: None }.into(),
        start_timer_cmd(1, Duration::from_secs(1)),
    ];
    let expected_command_types_emitted = Some(vec![
        CommandType::StartTimer,
        CommandType::CompleteWorkflowExecution,
    ]);
    _do_post_terminal_commands_test(
        commands_sent_by_lang,
        [ResponseType::ToTaskNum(1)],
        expected_command_types_emitted,
        t,
    )
    .await
}

#[tokio::test]
async fn post_terminal_commands_are_retained_when_replaying_and_flag_set() {
    // History contains a non-terminal command (N) followed by the terminal
    // command (T), with the MoveTerminalCommands flag set in the last WFT The
    // test establishes that, when core replays this history, it is consistent
    // with lang completing an activation with [T, N].
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_workflow_execution_completed();
    t.set_flags_last_wft(&[CoreInternalFlags::MoveTerminalCommands as u32], &[]);

    let commands_sent_by_lang = vec![
        CompleteWorkflowExecution { result: None }.into(),
        start_timer_cmd(1, Duration::from_secs(1)),
    ];

    _do_post_terminal_commands_test(commands_sent_by_lang, [ResponseType::AllHistory], None, t)
        .await
}

#[tokio::test]
async fn post_terminal_commands_are_not_retained_when_replaying_and_flag_not_set() {
    // History contains the terminal command (T) preceded immediately by
    // WFTCompleted, i.e. without any intervening non-terminal command (N), and
    // the MoveTerminalCommands flag is not set. The test establishes that when
    // core replays this history, it is consistent with lang completing an
    // activation with [T, N].
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let commands_sent_by_lang = vec![
        CompleteWorkflowExecution { result: None }.into(),
        start_timer_cmd(1, Duration::from_secs(1)),
    ];

    _do_post_terminal_commands_test(commands_sent_by_lang, [ResponseType::AllHistory], None, t)
        .await
}

async fn _do_post_terminal_commands_test(
    commands_sent_by_lang: Vec<workflow_command::Variant>,
    response_types: impl IntoIterator<Item = impl Into<ResponseType>>,
    expected_command_types: Option<Vec<CommandType>>,
    t: TestHistoryBuilder,
) {
    let mut mh =
        MockPollCfg::from_resp_batches("fake_wf_id", t, response_types, mock_worker_client());
    if let Some(expected_command_types) = expected_command_types {
        mh.num_expected_completions = Some(TimesRange::from(1));
        mh.completion_mock_fn = Some(Box::new(move |c| {
            let command_types: Vec<_> = c.commands.iter().map(|c| c.command_type()).collect();
            assert_eq!(command_types, expected_command_types);
            Ok(Default::default())
        }));
    } else {
        mh.num_expected_completions = Some(TimesRange::from(0));
    }
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        commands_sent_by_lang,
    ))
    .await
    .unwrap();

    core.initiate_shutdown();
    let act = core.poll_workflow_activation().await;
    assert_matches!(act.unwrap_err(), PollError::ShutDown);
    core.shutdown().await;
}

// Lang expects to always see jobs in this order:
//   patches, signals, everything else, queries
#[tokio::test]
async fn jobs_are_in_appropriate_order() {
    let p1 = "patchy-mc-patchface";
    let p2 = "enchi-the-kitty";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_has_change_marker(p1, false);
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_we_signaled("yummy-salmon", vec![]);
    t.add_full_wf_task();
    t.add_has_change_marker(p2, false);
    t.add_workflow_execution_completed();

    let mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory],
        mock_worker_client(),
    );
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant.as_ref().unwrap(),
        workflow_activation_job::Variant::InitializeWorkflow(_)
    );
    assert_matches!(
        act.jobs[1].variant.as_ref().unwrap(),
        workflow_activation_job::Variant::NotifyHasPatch(_)
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            SetPatchMarker {
                patch_id: p1.to_string(),
                deprecated: false,
            }
            .into(),
            start_timer_cmd(1, Duration::from_secs(1)),
        ],
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant.as_ref().unwrap(),
        workflow_activation_job::Variant::NotifyHasPatch(_)
    );
    assert_matches!(
        act.jobs[1].variant.as_ref().unwrap(),
        workflow_activation_job::Variant::SignalWorkflow(_)
    );
    assert_matches!(
        act.jobs[2].variant.as_ref().unwrap(),
        workflow_activation_job::Variant::FireTimer(_)
    );
}

#[rstest]
#[tokio::test]
async fn history_length_with_fail_and_timeout(
    #[values(true, false)] use_cache: bool,
    #[values(1, 2, 3)] history_responses_case: u8,
) {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(WorkflowTaskFailedCause::Unspecified, "ahh".into());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_client = mock_worker_client();
    let history_responses = match history_responses_case {
        1 => vec![ResponseType::AllHistory],
        2 => vec![
            ResponseType::ToTaskNum(1),
            ResponseType::ToTaskNum(2),
            ResponseType::AllHistory,
        ],
        3 => {
            let mut needs_fetch = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(2)).resp;
            needs_fetch.next_page_token = vec![1];
            // Truncate the history a bit in order to force incomplete WFT
            needs_fetch.history.as_mut().unwrap().events.truncate(6);
            let needs_fetch_resp = ResponseType::Raw(needs_fetch);
            let mut empty_fetch_resp: GetWorkflowExecutionHistoryResponse =
                t.get_history_info(1).unwrap().into();
            empty_fetch_resp.history.as_mut().unwrap().events = vec![];
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| Ok(empty_fetch_resp.clone()))
                .times(1);
            vec![
                ResponseType::ToTaskNum(1),
                needs_fetch_resp,
                ResponseType::ToTaskNum(2),
                ResponseType::AllHistory,
            ]
        }
        _ => unreachable!(),
    };

    let mut mh = MockPollCfg::from_resp_batches(wfid, t, history_responses, mock_client);
    if history_responses_case == 3 {
        // Expect the failed pagination fetch
        mh.num_expected_fails = 1;
    }
    let mut worker = mock_sdk_cfg(mh, |wc| {
        if use_cache {
            wc.max_cached_workflows = 1;
        }
    });
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        assert_eq!(ctx.history_length(), 3);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(ctx.history_length(), 14);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(ctx.history_length(), 19);
        Ok(().into())
    });
    worker
        .submit_wf(
            wfid.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn poller_wont_run_ahead_of_task_slots() {
    let popped_tasks = Arc::new(AtomicUsize::new(0));
    let ptc = popped_tasks.clone();
    let mut bunch_of_first_tasks = (1..50).map(move |i| {
        ptc.fetch_add(1, Ordering::Relaxed);
        hist_to_poll_resp(
            &canned_histories::single_timer(&format!("{i}")),
            format!("wf-{i}"),
            1.into(),
        )
        .resp
    });
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| Ok(bunch_of_first_tasks.next().unwrap()));
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| Ok(Default::default()));

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_cached_workflows(10_usize)
            .max_outstanding_workflow_tasks(10_usize)
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize))
            .no_remote_activities(true)
            .build()
            .unwrap(),
        mock_client,
    );

    // Should be able to get up to 10 tasks
    let mut tasks = vec![];
    for _ in 0..10 {
        tasks.push(worker.poll_workflow_activation().await.unwrap());
    }

    assert_eq!(worker.outstanding_workflow_tasks().await, 10);
    assert_eq!(worker.available_wft_permits(), Some(0));
    assert_eq!(worker.unused_wft_permits(), Some(0));

    // This one should hang until we complete some tasks since we're at the limit
    let hung_poll = async {
        // This should end up getting shut down after the other routine finishes tasks
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    };
    // Wait for a bit concurrently with above, verify no extra tasks got taken, shutdown
    let ender = async {
        time::sleep(Duration::from_millis(300)).await;
        // initiate shutdown, then complete open tasks
        worker.initiate_shutdown();
        for t in tasks {
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(t.run_id))
                .await
                .unwrap();
        }
        worker.shutdown().await;
    };
    join!(hung_poll, ender);
    // We shouldn't have got more than the 10 tasks from the poller -- verifying that the concurrent
    // polling is not exceeding the task limit
    assert_eq!(popped_tasks.load(Ordering::Relaxed), 10);
}

#[tokio::test]
async fn poller_wont_poll_until_lang_polls() {
    let mut mock_client = mock_worker_client();
    let (tx, rx) = sync_channel(101);
    // Normally you'd just not set any expectations, but the problem is since we never poll
    // the WFT stream, we'll never join the tasks running the pollers and thus the error
    // gets printed but doesn't bubble up to the test. So we set this explicit expectation
    // in here to ensure it isn't called.
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| {
            let _ = tx.send(());
            Ok(Default::default())
        });

    let worker = Worker::new_test(
        test_worker_cfg()
            .no_remote_activities(true)
            .build()
            .unwrap(),
        mock_client,
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    worker.drain_pollers_and_shutdown().await;
    // Nothing should've appeared here or we did poll
    assert!(rx.recv().is_err());
}

#[rstest]
#[tokio::test]
async fn use_compatible_version_flag(
    #[values(
        VersioningIntent::Unspecified,
        VersioningIntent::Compatible,
        VersioningIntent::Default
    )]
    intent: VersioningIntent,
    #[values(true, false)] different_tq: bool,
    #[values("activity", "child_wf", "continue_as_new")] command_type: &'static str,
) {
    let wfid = "fake_wf_id";
    let mut mock_client = mock_worker_client();
    let t = {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t
    };
    let compat_flag_expected = match intent {
        VersioningIntent::Unspecified => !different_tq,
        VersioningIntent::Compatible => true,
        VersioningIntent::Default => false,
    };
    #[allow(deprecated)]
    mock_client
        .expect_complete_workflow_task()
        .returning(move |mut c| {
            let can_cmd = c.commands.pop().unwrap().attributes.unwrap();
            match can_cmd {
                Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(a) => {
                    assert_eq!(a.inherit_build_id, compat_flag_expected);
                }
                Attributes::ScheduleActivityTaskCommandAttributes(a) => {
                    assert_eq!(a.use_workflow_build_id, compat_flag_expected);
                }
                Attributes::StartChildWorkflowExecutionCommandAttributes(a) => {
                    assert_eq!(a.inherit_build_id, compat_flag_expected);
                }
                _ => panic!("invalid attributes type"),
            }
            Ok(Default::default())
        });

    let mock = single_hist_mock_sg(wfid, t, vec![ResponseType::AllHistory], mock_client, true);
    let worker = mock_worker(mock);
    let r = worker.poll_workflow_activation().await.unwrap();
    let task_queue = if different_tq {
        "enchi cat!".to_string()
    } else {
        "".to_string()
    };
    let cmd = match command_type {
        "continue_as_new" => ContinueAsNewWorkflowExecution {
            workflow_type: "meow".to_string(),
            versioning_intent: intent as i32,
            task_queue,
            ..Default::default()
        }
        .into(),
        "activity" => ScheduleActivity {
            seq: 1,
            activity_id: "1".to_string(),
            versioning_intent: intent as i32,
            task_queue,
            ..default_act_sched()
        }
        .into(),
        "child_wf" => StartChildWorkflowExecution {
            seq: 1,
            versioning_intent: intent as i32,
            task_queue,
            ..Default::default()
        }
        .into(),
        _ => panic!("invalid command type"),
    };
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(r.run_id, cmd))
        .await
        .unwrap();
    worker.shutdown().await;
}

#[allow(deprecated)]
#[tokio::test]
async fn sets_build_id_from_wft_complete() {
    let wfid = "fake_wf_id";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.modify_event(t.current_event_id(), |he| {
        if let history_event::Attributes::WorkflowTaskCompletedEventAttributes(a) =
            he.attributes.as_mut().unwrap()
        {
            a.worker_version = Some(WorkerVersionStamp {
                build_id: "enchi-cat".to_string(),
                ..Default::default()
            });
        }
    });
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut worker = mock_sdk_cfg(
        MockPollCfg::from_resp_batches(wfid, t, [ResponseType::AllHistory], mock),
        |cfg| {
            cfg.versioning_strategy = WorkerVersioningStrategy::None {
                build_id: "fierce-predator".to_string(),
            };
            cfg.max_cached_workflows = 1;
        },
    );

    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        // First task, it should be empty, since replaying and nothing in first WFT completed
        assert_eq!(ctx.current_deployment_version(), None);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "enchi-cat"
        );
        ctx.timer(Duration::from_secs(1)).await;
        // Not replaying at this point, so we should see the worker's build id
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "fierce-predator"
        );
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "fierce-predator"
        );
        Ok(().into())
    });
    worker
        .submit_wf(wfid, DEFAULT_WORKFLOW_TYPE, vec![], Default::default())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn slot_provider_cant_hand_out_more_permits_than_cache_size() {
    let popped_tasks = Arc::new(AtomicUsize::new(0));
    let ptc = popped_tasks.clone();
    let mut bunch_of_first_tasks = (1..50).map(move |i| {
        ptc.fetch_add(1, Ordering::Relaxed);
        hist_to_poll_resp(
            &canned_histories::single_timer(&format!("{i}")),
            format!("wf-{i}"),
            1.into(),
        )
        .resp
    });
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| Ok(bunch_of_first_tasks.next().unwrap()));
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| Ok(Default::default()));

    struct EndlessSupplier {}
    #[async_trait::async_trait]
    impl SlotSupplier for EndlessSupplier {
        type SlotKind = WorkflowSlotKind;
        async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
            SlotSupplierPermit::default()
        }
        fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
            Some(SlotSupplierPermit::default())
        }
        fn mark_slot_used(&self, _ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}
        fn release_slot(&self, _: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {}
        fn available_slots(&self) -> Option<usize> {
            None
        }
    }

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_cached_workflows(10_usize)
            .tuner(Arc::new(
                TunerBuilder::default()
                    .workflow_slot_supplier(Arc::new(EndlessSupplier {}))
                    .build(),
            ))
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize))
            .no_remote_activities(true)
            .build()
            .unwrap(),
        mock_client,
    );

    // Should be able to get at 10 tasks
    let mut tasks = vec![];
    for _ in 0..10 {
        tasks.push(worker.poll_workflow_activation().await.unwrap());
    }
    // 11th should hang

    assert_eq!(worker.outstanding_workflow_tasks().await, 10);
    // assert_eq!(worker.available_wft_permits(), Some(0));
    // assert_eq!(worker.unused_wft_permits(), Some(0));

    // This one should hang until we complete some tasks since we're at the limit
    let hung_poll = async {
        // This should end up getting shut down after the other routine finishes tasks
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    };
    // Wait for a bit concurrently with above, verify no extra tasks got taken, shutdown
    let ender = async {
        time::sleep(Duration::from_millis(300)).await;
        // initiate shutdown, then complete open tasks
        worker.initiate_shutdown();
        for t in tasks {
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(t.run_id))
                .await
                .unwrap();
        }
        worker.shutdown().await;
    };
    join!(hung_poll, ender);
    // We shouldn't have got more than the 10 tasks from the poller -- verifying that the concurrent
    // polling is not exceeding the task limit
    assert_eq!(popped_tasks.load(Ordering::Relaxed), 10);
}

#[tokio::test]
async fn pass_timer_summary_to_metadata() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_user_metadata = Some(UserMetadata {
        summary: Some(b"timer summary".into()),
        details: None,
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                assert_eq!(wft.commands[0].user_metadata, expected_user_metadata)
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});
    worker.register_wf(wf_type, |ctx: WfContext| async move {
        ctx.timer(TimerOptions {
            duration: Duration::from_secs(1),
            summary: Some("timer summary".to_string()),
        })
        .await;
        Ok(().into())
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn both_normal_and_sticky_pollers_poll_concurrently() {
    struct Counters {
        // How many time PollWorkflowTaskQueue has been called
        normal_poll_count: AtomicUsize,
        sticky_poll_count: AtomicUsize,

        // How many pollers are currently active (i.e. PollWorkflowTaskQueue
        // has been called, but not the corresponding CompleteWorkflowTask)
        normal_slots_active_count: AtomicUsize,
        sticky_slots_active_count: AtomicUsize,

        // Max number of pollers that were active at the same time
        max_total_slots_active_count: AtomicUsize,
        max_normal_slots_active_count: AtomicUsize,
        max_sticky_slots_active_count: AtomicUsize,
    }

    let counters = Arc::new(Counters {
        normal_poll_count: AtomicUsize::new(0),
        sticky_poll_count: AtomicUsize::new(0),
        normal_slots_active_count: AtomicUsize::new(0),
        sticky_slots_active_count: AtomicUsize::new(0),
        max_total_slots_active_count: AtomicUsize::new(0),
        max_normal_slots_active_count: AtomicUsize::new(0),
        max_sticky_slots_active_count: AtomicUsize::new(0),
    });

    // Create actual workflow task responses to return from polls
    let mut task_responses = (1..100).map(|i| {
        hist_to_poll_resp(
            &canned_histories::single_timer(&format!("timer-{i}")),
            format!("wf-{i}"),
            1.into(),
        )
        .resp
    });

    let mut mock_client = mock_worker_client();

    // Track normal vs sticky poll requests and return actual workflow tasks
    let cc = Arc::clone(&counters);
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, opts: PollWorkflowOptions| {
            let mut task_response = task_responses.next().unwrap_or_default();

            // FIXME: Atomics initially made sense, but this has grown ugly, and there's probably
            // cases where this may produce incorrect results due to race in operation ordering
            // (really didn't put any thought into this). We also can't have
            if opts.sticky_queue_name.is_none() {
                // Normal queue poll
                cc.normal_poll_count.fetch_add(1, Ordering::Relaxed);
                cc.normal_slots_active_count.fetch_add(1, Ordering::Relaxed);
                cc.max_normal_slots_active_count.fetch_max(
                    cc.normal_slots_active_count.load(Ordering::Relaxed),
                    Ordering::AcqRel,
                );
                cc.max_total_slots_active_count.fetch_max(
                    cc.normal_slots_active_count.load(Ordering::Relaxed)
                        + cc.sticky_slots_active_count.load(Ordering::Relaxed),
                    Ordering::AcqRel,
                );

                task_response.task_token = [task_response.task_token, b"normal".to_vec()].concat();
            } else {
                // Sticky queue poll
                cc.sticky_poll_count.fetch_add(1, Ordering::Relaxed);
                cc.sticky_slots_active_count.fetch_add(1, Ordering::Relaxed);
                cc.max_sticky_slots_active_count.fetch_max(
                    cc.sticky_slots_active_count.load(Ordering::Acquire),
                    Ordering::AcqRel,
                );
                cc.max_total_slots_active_count.fetch_max(
                    cc.normal_slots_active_count.load(Ordering::Relaxed)
                        + cc.sticky_slots_active_count.load(Ordering::Relaxed),
                    Ordering::AcqRel,
                );

                task_response.task_token = [task_response.task_token, b"sticky".to_vec()].concat();
            }

            // Return actual workflow task responses
            Ok(task_response)
        });

    let cc = Arc::clone(&counters);
    mock_client
        .expect_complete_workflow_task()
        .returning(move |completion| {
            if completion.task_token.0.ends_with(b"normal") {
                cc.normal_slots_active_count.fetch_sub(1, Ordering::Relaxed);
            } else {
                cc.sticky_slots_active_count.fetch_sub(1, Ordering::Relaxed);
            }
            Ok(Default::default())
        });

    let worker = Worker::new(
        test_worker_cfg()
            .max_cached_workflows(500_usize) // We need cache, but don't want to deal with evictions
            .max_outstanding_workflow_tasks(2_usize)
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2_usize))
            .nonsticky_to_sticky_poll_ratio(0.2)
            .no_remote_activities(true)
            .build()
            .unwrap(),
        Some("stickytq".to_string()),
        Arc::new(mock_client),
        None,
        None,
    );

    for _ in 1..50 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        let _ = worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await;
    }

    assert!(
        counters.normal_poll_count.load(Ordering::Relaxed) > 0,
        "Normal poller should have been called at least once"
    );
    assert!(
        counters.sticky_poll_count.load(Ordering::Relaxed) > 0,
        "Sticky poller should have been called at least once"
    );
    assert!(
        counters
            .max_normal_slots_active_count
            .load(Ordering::Relaxed)
            >= 1,
        "Normal poller should have been active at least once"
    );
    assert!(
        counters
            .max_sticky_slots_active_count
            .load(Ordering::Relaxed)
            >= 1,
        "Sticky poller should have been active at least once"
    );
    assert_eq!(
        counters
            .max_total_slots_active_count
            .load(Ordering::Relaxed),
        2,
        "At peak, there should be exactly 2 pollers active at the same time"
    );
}
