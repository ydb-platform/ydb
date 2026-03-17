use crate::{
    test_help::{
        MockPollCfg, MocksHolder, ResponseType, WorkerExt, build_mock_pollers, canned_histories,
        hist_to_poll_resp, mock_worker, single_hist_mock_sg,
    },
    worker::{
        LEGACY_QUERY_ID,
        client::{LegacyQueryResult, mocks::mock_worker_client},
    },
};
use futures_util::stream;
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use temporal_sdk_core_api::{Worker as WorkerTrait, worker::WorkerVersioningStrategy};
use temporal_sdk_core_protos::{
    TestHistoryBuilder,
    coresdk::{
        workflow_activation::{
            WorkflowActivationJob, remove_from_cache::EvictionReason, workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, ContinueAsNewWorkflowExecution,
            QueryResult, RequestCancelActivity, query_result,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::{ActivityTaskCancelRequestedEventAttributes, History, history_event},
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporal_sdk_core_test_utils::{
    WorkerTestHelpers, query_ok, schedule_activity_cmd, start_timer_cmd,
};

#[rstest::rstest]
#[case::with_history(true)]
#[case::without_history(false)]
#[tokio::test]
async fn legacy_query(#[case] include_history: bool) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
    let mut header = HashMap::new();
    header.insert("head".to_string(), Payload::from(b"er"));
    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: Some(header.into()),
            });
            if !include_history {
                pr.history = Some(History { events: vec![] });
            }
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into()),
    ];
    let mut mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mock.num_expected_legacy_query_resps = 1;
    let mut mock = build_mock_pollers(mock);
    if !include_history {
        mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    }
    let worker = mock_worker(mock);

    let first_wft = || async {
        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let clear_eviction = || async {
        worker.handle_eviction().await;
    };

    first_wft().await;

    if include_history {
        clear_eviction().await;
        first_wft().await;
    }

    let task = worker.poll_workflow_activation().await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    let query = assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    assert_eq!(query.headers.get("head").unwrap(), &b"er".into());
    // Complete the query
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(&query.query_id, query_resp),
        ))
        .await
        .unwrap();

    if include_history {
        clear_eviction().await;
        first_wft().await;
    }

    let task = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn new_queries(#[values(1, 3)] num_queries: usize) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
    let mut header = HashMap::new();
    header.insert("head".to_string(), Payload::from(b"er"));
    let tasks = VecDeque::from(vec![hist_to_poll_resp(&t, wfid.to_owned(), 1.into()), {
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
        pr.queries = HashMap::new();
        for i in 1..=num_queries {
            pr.queries.insert(
                format!("q{i}"),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: Some(header.clone().into()),
                },
            );
        }
        pr
    }]);
    let mut mock_client = mock_worker_client();
    mock_client.expect_respond_legacy_query().times(0);
    let mut mh = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mh.completion_mock_fn = Some(Box::new(move |c| {
        // If the completion is the one ending the workflow, make sure it includes the query resps
        if c.commands[0].command_type() == CommandType::CompleteWorkflowExecution {
            assert_eq!(c.query_responses.len(), num_queries);
        } else if c.commands[0].command_type() == CommandType::StartTimer {
            // first reply, no queries here.
        } else {
            panic!("Unexpected command in response")
        }
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(mh);
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
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        CompleteWorkflowExecution { result: None }.into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    for i in 0..num_queries {
        assert_matches!(
            task.jobs[i],
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
            } => {
                assert_eq!(q.headers.get("head").unwrap(), &b"er".into());
            }
        );
    }

    let mut commands = vec![];
    for i in 1..=num_queries {
        commands.push(query_ok(format!("q{i}"), query_resp));
    }
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        commands,
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn legacy_query_failure_on_wft_failure() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = VecDeque::from(vec![hist_to_poll_resp(&t, wfid.to_owned(), 1.into()), {
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr.history = Some(History { events: vec![] });
        pr
    }]);
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
    // Poll again, and we end up getting a `query` field query response
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Fail wft which should result in query being failed
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        task.run_id,
        Failure {
            message: "Ahh i broke".to_string(),
            ..Default::default()
        },
        None,
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn query_failure_because_nondeterminism(#[values(true, false)] legacy: bool) {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = [{
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory);
        if legacy {
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
        } else {
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
        }
        pr
    }];
    let mut mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    if legacy {
        mock.expect_legacy_query_matcher = Box::new(|_, f| match f {
            LegacyQueryResult::Failed(f) => {
                f.force_cause() == WorkflowTaskFailedCause::NonDeterministicError
            }
            _ => false,
        });
        mock.num_expected_legacy_query_resps = 1;
    } else {
        mock.num_expected_fails = 1;
    }
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 10;
        wc.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    // Nondeterminism, should result in WFT being failed
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.handle_eviction().await;
    core.shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn legacy_query_after_complete(#[values(false, true)] full_history: bool) {
    let wfid = "fake_wf_id";
    let t = if full_history {
        canned_histories::single_timer_wf_completes("1")
    } else {
        let mut t = canned_histories::single_timer("1");
        t.add_workflow_task_completed();
        t
    };
    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory);
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr.resp
    };
    // Server would never send us a workflow task *without* a query that goes all the way to
    // execution completed. So, don't do that. It messes with the mock unlocking the next
    // task since we (appropriately) won't respond to server in that situation.
    let mut tasks = if full_history {
        vec![]
    } else {
        vec![hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory).resp]
    };
    tasks.extend([query_with_hist_task.clone(), query_with_hist_task]);

    let mut mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mock.num_expected_legacy_query_resps = 2;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let activations = || async {
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    };
    activations().await;

    if !full_history {
        core.handle_eviction().await;
        activations().await;
    }

    // We should get queries two times
    for i in 1..=2 {
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok(query.query_id.clone(), "whatever"),
        ))
        .await
        .unwrap();
        if i == 1 {
            core.handle_eviction().await;
            activations().await;
        }
    }

    core.shutdown().await;
}

enum QueryHists {
    Empty,
    Full,
    Partial,
}
#[rstest::rstest]
#[tokio::test]
async fn query_cache_miss_causes_page_fetch_dont_reply_wft_too_early(
    #[values(QueryHists::Empty, QueryHists::Full, QueryHists::Partial)] hist_type: QueryHists,
) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
    let full_hist = t.get_full_history_info().unwrap();
    let tasks = VecDeque::from(vec![{
        let mut pr = match hist_type {
            QueryHists::Empty => {
                // Create a no-history poll response. This happens to be easiest to do by just ripping
                // out the history after making a normal one.
                let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory);
                pr.history = Some(Default::default());
                pr
            }
            QueryHists::Full => hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory),
            QueryHists::Partial => {
                // Create a partial task
                hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2))
            }
        };
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
    }]);
    let mut mock_client = mock_worker_client();
    if !matches!(hist_type, QueryHists::Full) {
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(full_hist.clone().into()),
                    ..Default::default()
                })
            });
    }
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp| {
            // Verify both the complete command and the query response are sent
            assert_eq!(resp.commands.len(), 1);
            assert_eq!(resp.query_responses.len(), 1);

            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock_client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);
    let task = core.poll_workflow_activation().await.unwrap();
    // The first task should *only* start the workflow. It should *not* have a query in it, which
    // was the bug. Query should only appear after we have caught up on replay.
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
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
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        CompleteWorkflowExecution { result: None }.into(),
    ))
    .await
    .unwrap();

    // Now the query shall arrive
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs[0],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(_)),
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok("the-query".to_string(), query_resp),
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn query_replay_with_continue_as_new_doesnt_reply_empty_command() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1));
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
    let tasks = VecDeque::from(vec![query_with_hist_task]);
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp| {
            // Verify both the complete command and the query response are sent
            assert_eq!(resp.commands.len(), 1);
            assert_eq!(resp.query_responses.len(), 1);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock_client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    // Scheduling and immediately canceling an activity produces another activation which is
    // important in this repro
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            schedule_activity_cmd(
                0,
                "whatever",
                "act-id",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            RequestCancelActivity { seq: 0 }.into(),
            ContinueAsNewWorkflowExecution {
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    // Activity unblocked
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    let query = assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Throw an evict in there. Repro required a pending eviction during complete.
    core.request_wf_eviction(&task.run_id, "I said so", EvictionReason::LangRequested);

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(query.query_id.clone(), "whatever"),
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn legacy_query_response_gets_not_found_not_fatal() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = [{
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr
    }];
    let mut mock = mock_worker_client();
    mock.expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Err(tonic::Status::not_found("Query gone boi")));
    let mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock);
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
    // Poll again, and we end up getting a `query` field query response
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Fail wft which should result in query being failed
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID.to_string(), "hi"),
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn new_query_fail() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = VecDeque::from(vec![{
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
        pr.queries = HashMap::new();
        pr.queries.insert(
            "q1".to_string(),
            WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: Default::default(),
            },
        );
        pr
    }]);
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(|resp| {
            // Verify there is a failed query response along w/ start timer cmd
            assert_eq!(resp.commands.len(), 1);
            assert_matches!(
                resp.query_responses.as_slice(),
                &[QueryResult {
                    variant: Some(query_result::Variant::Failed(_)),
                    ..
                }]
            );
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });

    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock_client, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs[0],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }
    );

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs[0],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(_)),
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        QueryResult {
            query_id: "q1".to_string(),
            variant: Some(query_result::Variant::Failed("ahhh".into())),
        }
        .into(),
    ))
    .await
    .unwrap();
}

/// This test verifies that if we get a task with a legacy query in it while in the middle of
/// processing some local-only work (in this case, resolving an activity as soon as it was
/// cancelled) that we do not combine the legacy query with the resolve job.
#[tokio::test]
async fn legacy_query_combined_with_timer_fire_repro() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("1");
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add(
        history_event::Attributes::ActivityTaskCancelRequestedEventAttributes(
            ActivityTaskCancelRequestedEventAttributes {
                scheduled_event_id,
                ..Default::default()
            },
        ),
    );

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            // One task is super important here - as we need to look like we hit the cache
            // to apply this query right away
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
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
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(2));
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

    let activations = || async {
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![
                schedule_activity_cmd(
                    1,
                    "whatever",
                    "1",
                    ActivityCancellationType::TryCancel,
                    Duration::from_secs(60),
                    Duration::from_secs(60),
                ),
                start_timer_cmd(1, Duration::from_secs(1)),
            ],
        ))
        .await
        .unwrap();

        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            RequestCancelActivity { seq: 1 }.into(),
        ))
        .await
        .unwrap();

        // First should get the activity resolve
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_execution(&task.run_id).await;
    };
    activations().await;

    // Then the queries
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }]
        if q.query_id == "the-query"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok("the-query".to_string(), "whatever"),
    ))
    .await
    .unwrap();

    core.handle_eviction().await;
    activations().await;

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }]
        if q.query_id == LEGACY_QUERY_ID
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID.to_string(), "whatever"),
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn build_id_set_properly_on_query_on_first_task() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let tasks = VecDeque::from(vec![{
        let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::AllHistory);
        pr.queries.insert(
            "q".to_string(),
            WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            },
        );
        pr
    }]);
    let mut mock_client = mock_worker_client();
    mock_client.expect_respond_legacy_query().times(0);
    let mh = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 10;
        wc.versioning_strategy = WorkerVersioningStrategy::None {
            build_id: "1.0".to_owned(),
        }
    });
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        task.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "1.0"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        task.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "1.0"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    core.drain_pollers_and_shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn queries_arent_lost_in_buffer_void(#[values(false, true)] buffered_because_cache: bool) {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut tasks = if buffered_because_cache {
        // A history for another wf which will occupy the only cache slot initially
        let mut t1 = TestHistoryBuilder::default();
        t1.add_by_type(EventType::WorkflowExecutionStarted);
        t1.add_full_wf_task();
        t1.add_workflow_execution_completed();

        vec![hist_to_poll_resp(
            &t1,
            "cache_occupier".to_owned(),
            1.into(),
        )]
    } else {
        vec![]
    };

    tasks.extend([
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
            pr.query = Some(WorkflowQuery {
                query_type: "1".to_string(),
                ..Default::default()
            });
            pr.started_event_id = 0;
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into());
            pr.query = Some(WorkflowQuery {
                query_type: "2".to_string(),
                ..Default::default()
            });
            pr.started_event_id = 0;
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into()),
    ]);

    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .returning(|_| Ok(Default::default()));
    mock.expect_respond_legacy_query()
        .times(2)
        .returning(|_, _| Ok(Default::default()));
    let mut mock =
        MocksHolder::from_wft_stream(mock, stream::iter(tasks.into_iter().map(|r| r.resp)));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    if buffered_because_cache {
        // Poll for and complete the occupying workflow
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&task.run_id).await;
        // Get the cache removal task
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        // Wait a beat to ensure the other task(s) have a chance to be buffered
        tokio::time::sleep(Duration::from_millis(100)).await;
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
    }

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q.query_type == "1"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID.to_string(), "hi"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q.query_type == "2"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID.to_string(), "hi"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;

    core.shutdown().await;
}
