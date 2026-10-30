//! Unit test helpers - only available in unit tests (cfg(test))

use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{collections::HashSet, future::Future};
use temporalio_common::protos::coresdk::{
    workflow_activation::workflow_activation_job,
    workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
};

/// Given a desired number of concurrent executions and a provided function that produces a future,
/// run that many instances of the future concurrently.
///
/// Annoyingly, because of a sorta-bug in the way async blocks work, the async block produced by
/// the closure must be `async move` if it uses the provided iteration number. On the plus side,
/// since you're usually just accessing core in the closure, if core is a reference everything just
/// works. See <https://github.com/rust-lang/rust/issues/81653>
pub async fn fanout_tasks<FutureMaker, Fut>(num: usize, fm: FutureMaker)
where
    FutureMaker: Fn(usize) -> Fut,
    Fut: Future,
{
    let mut tasks = FuturesUnordered::new();
    for i in 0..num {
        tasks.push(fm(i));
    }

    while tasks.next().await.is_some() {}
}

/// Generate asserts for [poll_and_reply] by passing patterns to match against the job list
#[macro_export]
macro_rules! job_assert {
    ($($pat:pat),+) => {
        |res| {
            assert_matches!(
                res.jobs.as_slice(),
                [$(WorkflowActivationJob {
                    variant: Some($pat),
                }),+]
            );
        }
    };
}

type AsserterWithReply<'a> = (
    &'a dyn Fn(&temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation),
    workflow_activation_completion::Status,
);

/// Determines when workflows are kept in the cache or evicted for [poll_and_reply] type tests
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowCachingPolicy {
    /// Workflows are evicted after each workflow task completion. Note that this is *not* after
    /// each workflow activation - there are often multiple activations per workflow task.
    NonSticky,

    /// Not a real mode, but good for imitating crashes. Evict workflows after *every* reply,
    /// even if there are pending activations
    AfterEveryReply,
}

/// This function accepts a list of asserts and replies to workflow activations to run against the
/// provided instance of fake core.
///
/// It handles the business of re-sending the same activation replies over again in the event
/// of eviction or workflow activation failure. Activation failures specifically are only run once,
/// since they clearly can't be returned every time we replay the workflow, or it could never
/// proceed
pub(crate) async fn poll_and_reply<'a>(
    worker: &'a crate::Worker,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    poll_and_reply_clears_outstanding_evicts(worker, None, eviction_mode, expect_and_reply).await;
}

use crate::{pollers::MockPoller, test_help::OutstandingWFTMap};

pub(crate) async fn poll_and_reply_clears_outstanding_evicts<'a>(
    worker: &'a crate::Worker,
    outstanding_map: Option<OutstandingWFTMap>,
    eviction_mode: WorkflowCachingPolicy,
    expect_and_reply: &'a [AsserterWithReply<'a>],
) {
    let mut evictions = 0;
    let expected_evictions = expect_and_reply.len() - 1;
    let mut executed_failures = HashSet::new();
    let expected_fail_count = expect_and_reply
        .iter()
        .filter(|(_, reply)| !reply.is_success())
        .count();

    'outer: loop {
        let expect_iter = expect_and_reply.iter();

        for (i, interaction) in expect_iter.enumerate() {
            let (asserter, reply) = interaction;
            let complete_is_failure = !reply.is_success();
            // Only send activation failures once
            if executed_failures.contains(&i) {
                continue;
            }

            let mut res = worker.poll_workflow_activation().await.unwrap();
            if res.jobs.iter().any(|j| {
                matches!(
                    j.variant,
                    Some(workflow_activation_job::Variant::RemoveFromCache(_))
                )
            }) && res.jobs.len() > 1
            {
                panic!("Saw an activation with an eviction & other work! {res:?}");
            }
            let is_eviction = res.is_only_eviction();

            let mut do_release = false;

            if is_eviction {
                // If the job is an eviction, clear it, since in the tests we don't explicitly
                // specify evict assertions
                res.jobs.clear();
                do_release = true;
            }

            // TODO: Can remove this if?
            if !res.jobs.is_empty() {
                asserter(&res);
            }

            let reply = if res.jobs.is_empty() {
                // Just an eviction
                WorkflowActivationCompletion::empty(res.run_id.clone())
            } else {
                // Eviction plus some work, we still want to issue the reply
                WorkflowActivationCompletion {
                    run_id: res.run_id.clone(),
                    status: Some(reply.clone()),
                    ..Default::default()
                }
            };

            let ends_execution = reply.has_execution_ending();

            worker.complete_workflow_activation(reply).await.unwrap();

            if do_release && let Some(omap) = outstanding_map.as_ref() {
                omap.release_run(&res.run_id);
            }
            // Restart assertions from the beginning if it was an eviction (and workflow execution
            // isn't over)
            if is_eviction && !ends_execution {
                continue 'outer;
            }

            if complete_is_failure {
                executed_failures.insert(i);
            }

            match eviction_mode {
                WorkflowCachingPolicy::NonSticky => (),
                WorkflowCachingPolicy::AfterEveryReply => {
                    if evictions < expected_evictions {
                        worker.request_workflow_eviction(&res.run_id);
                        evictions += 1;
                    }
                }
            }
        }

        break;
    }

    assert_eq!(expected_fail_count, executed_failures.len());
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
}

pub(crate) fn gen_assert_and_reply(
    asserter: &dyn Fn(&temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation),
    reply_commands: Vec<
        temporalio_common::protos::coresdk::workflow_commands::workflow_command::Variant,
    >,
) -> AsserterWithReply<'_> {
    (
        asserter,
        temporalio_common::protos::coresdk::workflow_completion::Success::from_variants(
            reply_commands,
        )
        .into(),
    )
}

pub(crate) fn gen_assert_and_fail(
    asserter: &dyn Fn(&temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation),
) -> AsserterWithReply<'_> {
    (
        asserter,
        temporalio_common::protos::coresdk::workflow_completion::Failure {
            failure: Some(
                temporalio_common::protos::temporal::api::failure::v1::Failure {
                    message: "Intentional test failure".to_string(),
                    ..Default::default()
                },
            ),
            ..Default::default()
        }
        .into(),
    )
}

pub(crate) fn mock_poller<T>() -> MockPoller<T>
where
    T: Send + Sync + 'static,
{
    let mut mock_poller = MockPoller::new();
    mock_poller.expect_shutdown_box().return_const(());
    mock_poller.expect_notify_shutdown().return_const(());
    mock_poller
}
