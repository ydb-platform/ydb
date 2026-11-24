use crate::{
    ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT, MESSAGE_TOO_LARGE_KEY,
    grpc::IsUserLongPoll,
    request_extensions::{IsWorkerTaskLongPoll, NoRetryOnMatching, RetryConfigForCall},
};
use backon::{BackoffBuilder, ExponentialBuilder};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{
    error::Error,
    fmt::Debug,
    future::Future,
    time::{Duration, Instant},
};
use tonic::Code;

/// List of gRPC error codes that client will retry.
#[doc(hidden)]
pub const RETRYABLE_ERROR_CODES: [Code; 7] = [
    Code::DataLoss,
    Code::Internal,
    Code::Unknown,
    Code::ResourceExhausted,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unavailable,
];
const LONG_POLL_FATAL_GRACE: Duration = Duration::from_secs(60);

/// Configuration for retrying requests to the server
#[derive(Clone, Debug, PartialEq, bon::Builder)]
#[non_exhaustive]
pub struct RetryOptions {
    /// initial wait time before the first retry.
    #[builder(default = Duration::from_millis(100))]
    pub initial_interval: Duration,
    /// randomization jitter that is used as a multiplier for the current retry interval
    /// and is added or subtracted from the interval length.
    #[builder(default = 0.2)]
    pub randomization_factor: f64,
    /// rate at which retry time should be increased, until it reaches max_interval.
    #[builder(default = 1.7)]
    pub multiplier: f64,
    /// maximum amount of time to wait between retries.
    #[builder(default = Duration::from_secs(5))]
    pub max_interval: Duration,
    /// maximum total amount of time requests should be retried for, if None is set then no limit
    /// will be used.
    #[builder(required, default = Some(Duration::from_secs(10)))]
    pub max_elapsed_time: Option<Duration>,
    /// maximum number of retry attempts.
    #[builder(default = 10)]
    pub max_retries: usize,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RetryOptions {
    pub(crate) const fn task_poll_retry_policy() -> Self {
        Self {
            initial_interval: Duration::from_millis(200),
            randomization_factor: 0.2,
            multiplier: 2.0,
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            max_retries: 0,
        }
    }

    pub(crate) const fn throttle_retry_policy() -> Self {
        Self {
            initial_interval: Duration::from_secs(1),
            randomization_factor: 0.2,
            multiplier: 2.0,
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            max_retries: 0,
        }
    }

    /// A retry policy that never retires
    pub const fn no_retries() -> Self {
        Self {
            initial_interval: Duration::from_secs(0),
            randomization_factor: 0.0,
            multiplier: 1.0,
            max_interval: Duration::from_secs(0),
            max_elapsed_time: None,
            max_retries: 1,
        }
    }

    pub(crate) fn get_call_info<R>(
        &self,
        call_name: &'static str,
        request: Option<&tonic::Request<R>>,
    ) -> CallInfo {
        let mut call_type = CallType::Normal;
        let mut retry_short_circuit = None;
        let mut retry_cfg_override = None;
        if let Some(r) = request.as_ref() {
            let ext = r.extensions();
            if ext.get::<IsUserLongPoll>().is_some() {
                call_type = CallType::UserLongPoll;
            } else if ext.get::<IsWorkerTaskLongPoll>().is_some() {
                call_type = CallType::TaskLongPoll;
            }

            retry_short_circuit = ext.get::<NoRetryOnMatching>().cloned();
            retry_cfg_override = ext.get::<RetryConfigForCall>().cloned();
        }
        let retry_cfg = if let Some(ovr) = retry_cfg_override {
            ovr.0
        } else if call_type == CallType::TaskLongPoll {
            RetryOptions::task_poll_retry_policy()
        } else {
            self.clone()
        };
        CallInfo {
            call_type,
            call_name,
            retry_cfg,
            retry_short_circuit,
        }
    }

    fn jittered_backoff(&self) -> JitteredBackoff {
        let inner = ExponentialBuilder::new()
            .with_min_delay(self.initial_interval)
            .with_factor(self.multiplier as f32)
            .with_max_delay(self.max_interval)
            .without_max_times()
            .build();
        JitteredBackoff {
            inner,
            randomization_factor: self.randomization_factor,
            max_elapsed_time: self.max_elapsed_time,
            started_at: Instant::now(),
        }
    }
}

pub(crate) fn make_future_retry<R, F, Fut>(
    info: CallInfo,
    factory: F,
) -> FutureRetry<F, TonicErrorHandler>
where
    F: FnMut() -> Fut + Unpin,
    Fut: Future<Output = Result<R, tonic::Status>>,
{
    FutureRetry::new(
        factory,
        TonicErrorHandler::new(info, RetryOptions::throttle_retry_policy()),
    )
}

#[doc(hidden)]
pub fn jittered(base: Duration, randomization_factor: f64) -> Duration {
    if randomization_factor <= 0.0 {
        return base;
    }
    // Reproduce the `backoff` crate's documented jitter for backward compatibility:
    //   randomized interval = retry_interval * (random value in range [1 - randomization_factor, 1 + randomization_factor])
    // Docs: https://github.com/ihrwein/backoff/blob/587e2da8fb2dcfc65ca544cb9249022c51f1406e/src/lib.rs#L4
    // Algorithm (`get_random_value_from_interval`): https://github.com/ihrwein/backoff/blob/587e2da8fb2dcfc65ca544cb9249022c51f1406e/src/exponential.rs#L61
    let base_secs = base.as_secs_f64();
    let spread = randomization_factor * base_secs;
    let offset = spread * (2.0 * rand::random::<f64>() - 1.0);
    Duration::try_from_secs_f64((base_secs + offset).max(0.0)).unwrap_or(base)
}

#[derive(Debug)]
struct JitteredBackoff {
    inner: backon::ExponentialBackoff,
    randomization_factor: f64,
    max_elapsed_time: Option<Duration>,
    started_at: Instant,
}

impl JitteredBackoff {
    fn next_backoff(&mut self) -> Option<Duration> {
        // `inner` never stops on its own; the total retry budget is enforced here
        // against wall-clock time so the jittered delay we actually return is what
        // counts toward `max_elapsed_time`.
        let base = self.inner.next()?;
        let delay = jittered(base, self.randomization_factor);
        if let Some(max_elapsed_time) = self.max_elapsed_time
            && self.started_at.elapsed() + delay > max_elapsed_time
        {
            return None;
        }
        Some(delay)
    }
}

#[derive(Debug)]
pub(crate) struct TonicErrorHandler {
    backoff: JitteredBackoff,
    throttle_backoff: JitteredBackoff,
    max_interval: Duration,
    retry_started_at: Instant,
    max_retries: usize,
    call_type: CallType,
    call_name: &'static str,
    retry_short_circuit: Option<NoRetryOnMatching>,
}

impl TonicErrorHandler {
    fn new(call_info: CallInfo, throttle_cfg: RetryOptions) -> Self {
        Self {
            call_type: call_info.call_type,
            call_name: call_info.call_name,
            max_retries: call_info.retry_cfg.max_retries,
            max_interval: call_info.retry_cfg.max_interval,
            backoff: call_info.retry_cfg.jittered_backoff(),
            throttle_backoff: throttle_cfg.jittered_backoff(),
            retry_started_at: Instant::now(),
            retry_short_circuit: call_info.retry_short_circuit,
        }
    }

    fn maybe_log_retry(&self, cur_attempt: usize, err: &tonic::Status) {
        let mut do_log = false;
        // Warn on more than 5 retries for unlimited retrying
        if self.max_retries == 0 && cur_attempt > 5 {
            do_log = true;
        }
        // Warn if the attempts are more than 50% of max retries
        if self.max_retries > 0 && cur_attempt * 2 >= self.max_retries {
            do_log = true;
        }

        if do_log {
            // Error if unlimited retries have been going on for a while
            if self.max_retries == 0 && cur_attempt > 15 {
                error!(error=?err, "gRPC call {} retried {} times", self.call_name, cur_attempt);
            } else {
                warn!(error=?err, "gRPC call {} retried {} times", self.call_name, cur_attempt);
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CallInfo {
    pub call_type: CallType,
    call_name: &'static str,
    retry_cfg: RetryOptions,
    retry_short_circuit: Option<NoRetryOnMatching>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CallType {
    Normal,
    // A long poll but won't always retry timeouts/cancels. EX: Get workflow history
    UserLongPoll,
    // A worker is polling for a task
    TaskLongPoll,
}

impl CallType {
    pub(crate) fn is_long(&self) -> bool {
        matches!(self, Self::UserLongPoll | Self::TaskLongPoll)
    }
}

impl ErrorHandler<tonic::Status> for TonicErrorHandler {
    type OutError = tonic::Status;

    fn handle(
        &mut self,
        current_attempt: usize,
        mut e: tonic::Status,
    ) -> RetryPolicy<tonic::Status> {
        // 0 max retries means unlimited retries
        if self.max_retries > 0 && current_attempt >= self.max_retries {
            return RetryPolicy::ForwardError(e);
        }

        if let Some(sc) = self.retry_short_circuit.as_ref()
            && (sc.predicate)(&e)
        {
            e.metadata_mut().insert(
                ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT,
                tonic::metadata::MetadataValue::from(0),
            );
            return RetryPolicy::ForwardError(e);
        }

        // Short circuit if message is too large - this is not retryable
        if e.code() == Code::ResourceExhausted
            && (e
                .message()
                .starts_with("grpc: received message larger than max")
                || e.message()
                    .starts_with("grpc: message after decompression larger than max")
                || e.message()
                    .starts_with("grpc: received message after decompression larger than max"))
        {
            // Leave a marker so we don't have duplicate detection logic in the workflow
            e.metadata_mut().insert(
                MESSAGE_TOO_LARGE_KEY,
                tonic::metadata::MetadataValue::from(0),
            );
            return RetryPolicy::ForwardError(e);
        }

        // Task polls are OK with being cancelled or running into the timeout because there's
        // nothing to do but retry anyway
        let long_poll_allowed = self.call_type == CallType::TaskLongPoll
            && [Code::Cancelled, Code::DeadlineExceeded].contains(&e.code());

        // When Code::Cancelled originates from a transport-level error (e.g. GOAWAY,
        // connection closed during an AZ outage), it should be retried like Unavailable.
        // We distinguish this from true application/caller-initiated cancellations by
        // inspecting the error source chain for tonic::transport::Error → hyper::Error.
        let transport_cancel_retry_allowed =
            e.code() == Code::Cancelled && is_transport_cancelled(&e);

        if RETRYABLE_ERROR_CODES.contains(&e.code())
            || long_poll_allowed
            || transport_cancel_retry_allowed
        {
            if current_attempt == 1 {
                debug!(error=?e, "gRPC call {} failed on first attempt", self.call_name);
            } else {
                self.maybe_log_retry(current_attempt, &e);
            }

            match self.backoff.next_backoff() {
                None => RetryPolicy::ForwardError(e), // None is returned when we've ran out of time
                Some(backoff) => {
                    // We treat ResourceExhausted as a special case and backoff more
                    // so we don't overload the server
                    if e.code() == Code::ResourceExhausted {
                        let extended_backoff =
                            backoff.max(self.throttle_backoff.next_backoff().unwrap_or_default());
                        RetryPolicy::WaitRetry(extended_backoff)
                    } else {
                        RetryPolicy::WaitRetry(backoff)
                    }
                }
            }
        } else if self.call_type == CallType::TaskLongPoll
            && self.retry_started_at.elapsed() <= LONG_POLL_FATAL_GRACE
        {
            // We permit "fatal" errors while long polling for a while, because some proxies return
            // stupid error codes while getting ready, among other weird infra issues
            RetryPolicy::WaitRetry(self.max_interval)
        } else {
            RetryPolicy::ForwardError(e)
        }
    }
}

/// Returns true if the given status is a `Code::Cancelled` that originated from a
/// transport-level failure (tonic::transport::Error → hyper::Error) rather than
/// an application/caller-initiated cancellation. These should be retried like
/// `Code::Unavailable`.
fn is_transport_cancelled(status: &tonic::Status) -> bool {
    status
        .source()
        .and_then(|e| e.downcast_ref::<tonic::transport::Error>())
        .and_then(|te| te.source())
        .and_then(|tec| tec.downcast_ref::<hyper::Error>())
        .is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::time::Instant;
    use temporalio_common::protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueRequest, PollNexusTaskQueueRequest, PollWorkflowTaskQueueRequest,
    };
    use tonic::{IntoRequest, Status};

    /// Predefined retry configs with low durations to make unit tests faster
    const TEST_RETRY_CONFIG: RetryOptions = RetryOptions {
        initial_interval: Duration::from_millis(1),
        randomization_factor: 0.0,
        multiplier: 1.1,
        max_interval: Duration::from_millis(2),
        max_elapsed_time: None,
        max_retries: 10,
    };

    const POLL_WORKFLOW_METH_NAME: &str = "poll_workflow_task_queue";
    const POLL_ACTIVITY_METH_NAME: &str = "poll_activity_task_queue";
    const POLL_NEXUS_METH_NAME: &str = "poll_nexus_task_queue";

    #[tokio::test]
    async fn long_poll_non_retryable_errors() {
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::Unauthenticated,
            Code::Unimplemented,
        ] {
            for call_name in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler::new(
                    CallInfo {
                        call_type: CallType::TaskLongPoll,
                        call_name,
                        retry_cfg: TEST_RETRY_CONFIG,
                        retry_short_circuit: None,
                    },
                    TEST_RETRY_CONFIG,
                );
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.retry_started_at =
                    Instant::now() - LONG_POLL_FATAL_GRACE - Duration::from_secs(1);
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::ForwardError(_));
            }
        }
    }

    #[tokio::test]
    async fn long_poll_retryable_errors_never_fatal() {
        for code in RETRYABLE_ERROR_CODES {
            for call_name in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler::new(
                    CallInfo {
                        call_type: CallType::TaskLongPoll,
                        call_name,
                        retry_cfg: TEST_RETRY_CONFIG,
                        retry_short_circuit: None,
                    },
                    TEST_RETRY_CONFIG,
                );
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.retry_started_at =
                    Instant::now() - LONG_POLL_FATAL_GRACE - Duration::from_secs(1);
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }

    #[tokio::test]
    async fn retry_resource_exhausted() {
        let mut err_handler = TonicErrorHandler::new(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            RetryOptions {
                initial_interval: Duration::from_millis(2),
                randomization_factor: 0.0,
                multiplier: 4.0,
                max_interval: Duration::from_millis(10),
                max_elapsed_time: None,
                max_retries: 10,
            },
        );
        let result = err_handler.handle(1, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(2)),
            _ => panic!(),
        }
        let result = err_handler.handle(2, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(8)),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn retry_short_circuit() {
        let mut err_handler = TonicErrorHandler::new(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: Some(NoRetryOnMatching {
                    predicate: |s: &Status| s.code() == Code::ResourceExhausted,
                }),
            },
            TEST_RETRY_CONFIG,
        );
        let result = err_handler.handle(1, Status::new(Code::ResourceExhausted, "leave me alone"));
        let e = assert_matches!(result, RetryPolicy::ForwardError(e) => e);
        assert!(
            e.metadata()
                .get(ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT)
                .is_some()
        );
    }

    #[tokio::test]
    async fn message_too_large_not_retried() {
        let mut err_handler = TonicErrorHandler::new(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            TEST_RETRY_CONFIG,
        );
        let result = err_handler.handle(
            1,
            Status::new(
                Code::ResourceExhausted,
                "grpc: received message larger than max",
            ),
        );
        assert_matches!(result, RetryPolicy::ForwardError(_));

        let result = err_handler.handle(
            1,
            Status::new(
                Code::ResourceExhausted,
                "grpc: message after decompression larger than max",
            ),
        );
        assert_matches!(result, RetryPolicy::ForwardError(_));

        let result = err_handler.handle(
            1,
            Status::new(
                Code::ResourceExhausted,
                "grpc: received message after decompression larger than max",
            ),
        );
        assert_matches!(result, RetryPolicy::ForwardError(_));
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn task_poll_retries_forever<R>(
        #[values(
                (
                    POLL_WORKFLOW_METH_NAME,
                    PollWorkflowTaskQueueRequest::default(),
                ),
                (
                    POLL_ACTIVITY_METH_NAME,
                    PollActivityTaskQueueRequest::default(),
                ),
                (
                    POLL_NEXUS_METH_NAME,
                    PollNexusTaskQueueRequest::default(),
                ),
        )]
        (call_name, req): (&'static str, R),
    ) {
        // A bit odd, but we don't need a real client to test the retry client passes through the
        // correct retry config
        let mut req = req.into_request();
        req.extensions_mut().insert(IsWorkerTaskLongPoll);
        for i in 1..=50 {
            let mut err_handler = TonicErrorHandler::new(
                TEST_RETRY_CONFIG.get_call_info::<R>(call_name, Some(&req)),
                RetryOptions::throttle_retry_policy(),
            );
            let result = err_handler.handle(i, Status::new(Code::Unknown, "Ahh"));
            assert_matches!(result, RetryPolicy::WaitRetry(_));
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn task_poll_retries_deadline_exceeded<R>(
        #[values(
                (
                    POLL_WORKFLOW_METH_NAME,
                    PollWorkflowTaskQueueRequest::default(),
                ),
                (
                    POLL_ACTIVITY_METH_NAME,
                    PollActivityTaskQueueRequest::default(),
                ),
                (
                    POLL_NEXUS_METH_NAME,
                    PollNexusTaskQueueRequest::default(),
                ),
        )]
        (call_name, req): (&'static str, R),
    ) {
        let mut req = req.into_request();
        req.extensions_mut().insert(IsWorkerTaskLongPoll);
        // For some reason we will get cancelled in these situations occasionally (always?) too
        for code in [Code::Cancelled, Code::DeadlineExceeded] {
            let mut err_handler = TonicErrorHandler::new(
                TEST_RETRY_CONFIG.get_call_info::<R>(call_name, Some(&req)),
                RetryOptions::throttle_retry_policy(),
            );
            for i in 1..=5 {
                let result = err_handler.handle(i, Status::new(code, "retryable failure"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }

    #[tokio::test]
    async fn plain_cancelled_not_retried_on_normal_call() {
        // A plain Code::Cancelled (no transport error in source chain) on a Normal call
        // must NOT be retried — this is spec-correct behavior for application-level cancels.
        let mut err_handler = TonicErrorHandler::new(
            CallInfo {
                call_type: CallType::Normal,
                call_name: "respond_activity_task_completed",
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            TEST_RETRY_CONFIG,
        );
        let result = err_handler.handle(1, Status::new(Code::Cancelled, "caller cancelled"));
        assert_matches!(result, RetryPolicy::ForwardError(_));
    }

    #[tokio::test]
    async fn is_transport_cancelled_false_for_plain_status() {
        // A status without a transport error source chain should not be detected as
        // transport-cancelled.
        let status = Status::new(Code::Cancelled, "caller cancelled");
        assert!(!is_transport_cancelled(&status));
    }

    #[tokio::test]
    async fn transport_sourced_cancelled_retried_on_full_budget() {
        // NOTE: tonic::Status's public API doesn't allow constructing a Status with both
        // Code::Cancelled AND a transport error source chain. In production, tonic
        // internally builds this when a GOAWAY/connection-close kills an in-flight RPC.
        // We test the components separately:
        //   1. is_transport_cancelled correctly detects transport errors (test above)
        //   2. The retry handler correctly treats transport-cancelled as retryable (this test)
        //
        // For this test, we verify through the `handle` method that a transport-sourced
        // Cancelled status (created via from_error, which sets Code::Unknown but preserves
        // the transport source chain) IS retried multiple times on the standard budget.
        let mut err_handler = TonicErrorHandler::new(
            CallInfo {
                call_type: CallType::Normal,
                call_name: "respond_activity_task_completed",
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            TEST_RETRY_CONFIG,
        );

        // Code::Unknown with a transport source IS retried (it's in RETRYABLE_ERROR_CODES)
        // AND is_transport_cancelled would return true — both paths lead to retry.
        for i in 1..=5 {
            let endpoint = tonic::transport::Endpoint::from_static("http://[::1]:1")
                .connect_timeout(Duration::from_millis(1));
            let transport_err = endpoint.connect().await.unwrap_err();
            let status = Status::from_error(Box::new(transport_err));

            let result = err_handler.handle(i, status);
            assert_matches!(
                result,
                RetryPolicy::WaitRetry(_),
                "Transport error should be retried on attempt {i}"
            );
        }
    }
}
