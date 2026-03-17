use crate::{
    prost_dur,
    test_help::{
        MockPollCfg, PollWFTRespExt, ResponseType, build_mock_pollers, hist_to_poll_resp,
        mock_worker,
    },
    worker::client::mocks::mock_worker_client,
};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    DEFAULT_ACTIVITY_TYPE, TestHistoryBuilder,
    coresdk::{
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{
            CompleteWorkflowExecution, ScheduleActivity, StartTimer, UpdateResponse,
            update_response::Response,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        enums::v1::EventType,
        update::v1::{Acceptance, Rejection},
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};
use temporal_sdk_core_test_utils::WorkerTestHelpers;

#[tokio::test]
async fn replay_with_empty_first_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    let accept_id = t.add_update_accepted("upd1", "update");
    t.add_we_signaled("hi", vec![]);
    t.add_full_wf_task();
    t.add_update_completed(accept_id);
    t.add_workflow_execution_completed();

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    // In this task imagine we are waiting on the first update being sent, hence no commands come
    // out, and on replay the first activation should only be init.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: "upd1".to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
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
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Payload::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn initial_request_sent_back(#[values(false, true)] reject: bool) {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    let upd_req_body = poll_resp.add_update_request(update_id, 1);

    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(move |mut resp| {
            let msg = resp.messages.pop().unwrap();
            let orig_req = if reject {
                let acceptance = msg.body.unwrap().unpack_as(Rejection::default()).unwrap();
                acceptance.rejected_request.unwrap()
            } else {
                let acceptance = msg.body.unwrap().unpack_as(Acceptance::default()).unwrap();
                acceptance.accepted_request.unwrap()
            };
            assert_eq!(orig_req, upd_req_body);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_client);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    let resp = if reject {
        Response::Rejected(Default::default())
    } else {
        Response::Accepted(())
    };
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(resp),
        }
        .into(),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn speculative_wft_with_command_event() {
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_activity_task_scheduled("act1");

    let mut spec_task_hist = t.clone();
    spec_task_hist.add_workflow_task_scheduled_and_started();

    let mut real_hist = t.clone();
    real_hist.add_we_signaled("hi", vec![]);
    real_hist.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut speculative_task = hist_to_poll_resp(&spec_task_hist, wfid, ResponseType::OneTask(2));
    speculative_task.add_update_request(update_id, 1);
    // Verify the speculative task contains the activity scheduled event
    assert_eq!(
        speculative_task.history.as_ref().unwrap().events[1].event_type,
        EventType::ActivityTaskScheduled as i32
    );

    let mock_client = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wfid,
        real_hist,
        [
            ResponseType::ToTaskNum(1),
            speculative_task.into(),
            ResponseType::OneTask(3),
        ],
        mock_client,
    );
    let mut completes = 0;
    mh.completion_mock_fn = Some(Box::new(move |_| {
        completes += 1;
        let mut r = RespondWorkflowTaskCompletedResponse::default();
        if completes == 2 {
            // The second response (the update rejection) needs to indicate that the last started
            // wft ID should be reset.
            r.reset_history_event_id = 3;
        }
        Ok(r)
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            activity_id: "act1".to_string(),
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    // Receive the task containing and reject the update
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(Response::Rejected(Default::default())),
        }
        .into(),
    ))
    .await
    .unwrap();

    // Now we'll get another task with the "real" history containing the signal
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn replay_with_signal_and_update_same_task() {
    // Imitating a signal creating a command before update validator runs
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("hi", vec![]);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    let accept_id = t.add_update_accepted("upd1", "update");
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_update_completed(accept_id);
    t.add_workflow_execution_completed();

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    // In this task imagine we are waiting on the first update being sent, hence no commands come
    // out, and on replay the first activation should only be init.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            }
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Accepted(())),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Payload::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}
