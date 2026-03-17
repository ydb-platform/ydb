use crate::{
    job_assert,
    test_help::{
        ResponseType, WorkflowCachingPolicy::NonSticky, build_fake_worker, canned_histories,
        gen_assert_and_reply, poll_and_reply,
    },
};
use rstest::rstest;
use std::time::Duration;
use temporal_sdk_core_protos::coresdk::{
    workflow_activation::{WorkflowActivationJob, workflow_activation_job},
    workflow_commands::{
        CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution,
    },
};
use temporal_sdk_core_test_utils::start_timer_cmd;

enum CompletionType {
    Complete,
    Fail,
    Cancel,
}

#[rstest]
#[case::incremental_cancel(vec![1.into(), ResponseType::AllHistory], CompletionType::Cancel)]
#[case::replay_cancel(vec![ResponseType::AllHistory], CompletionType::Cancel)]
#[case::incremental_complete(vec![1.into(), ResponseType::AllHistory], CompletionType::Complete)]
#[case::replay_complete(vec![ResponseType::AllHistory], CompletionType::Complete)]
#[case::incremental_fail(vec![1.into(), ResponseType::AllHistory], CompletionType::Fail)]
#[case::replay_fail(vec![ResponseType::AllHistory], CompletionType::Fail)]
#[tokio::test]
async fn timer_then_cancel_req(
    #[case] hist_batches: Vec<ResponseType>,
    #[case] completion_type: CompletionType,
) {
    let wfid = "fake_wf_id";
    let timer_seq = 1;
    let timer_id = timer_seq.to_string();
    let t = match completion_type {
        CompletionType::Complete => {
            canned_histories::timer_wf_cancel_req_completed(timer_id.as_str())
        }
        CompletionType::Fail => canned_histories::timer_wf_cancel_req_failed(timer_id.as_str()),
        CompletionType::Cancel => {
            canned_histories::timer_wf_cancel_req_cancelled(timer_id.as_str())
        }
    };
    let core = build_fake_worker(wfid, t, hist_batches);

    let final_cmd = match completion_type {
        CompletionType::Complete => CompleteWorkflowExecution::default().into(),
        CompletionType::Fail => FailWorkflowExecution::default().into(),
        CompletionType::Cancel => CancelWorkflowExecution::default().into(),
    };

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![start_timer_cmd(timer_seq, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::FireTimer(_),
                    workflow_activation_job::Variant::CancelWorkflow(_)
                ),
                vec![final_cmd],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn timer_then_cancel_req_then_timer_then_cancelled() {
    let wfid = "fake_wf_id";
    let t = canned_histories::timer_wf_cancel_req_do_another_timer_then_cancelled();
    let core = build_fake_worker(wfid, t, [ResponseType::AllHistory]);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::InitializeWorkflow(_)),
                vec![start_timer_cmd(1, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::FireTimer(_),
                    workflow_activation_job::Variant::CancelWorkflow(_)
                ),
                vec![start_timer_cmd(2, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CancelWorkflowExecution::default().into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn immediate_cancel() {
    let wfid = "fake_wf_id";
    let t = canned_histories::immediate_wf_cancel();
    let core = build_fake_worker(wfid, t, [1]);

    poll_and_reply(
        &core,
        NonSticky,
        &[gen_assert_and_reply(
            &job_assert!(
                workflow_activation_job::Variant::InitializeWorkflow(_),
                workflow_activation_job::Variant::CancelWorkflow(_)
            ),
            vec![CancelWorkflowExecution {}.into()],
        )],
    )
    .await;
}
