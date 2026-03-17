use crate::{
    Client, ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT, IsWorkerTaskLongPoll, MESSAGE_TOO_LARGE_KEY,
    NamespacedClient, NoRetryOnMatching, Result, RetryConfig, raw::IsUserLongPoll,
};
use backoff::{Clock, SystemClock, backoff::Backoff, exponential::ExponentialBackoff};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{error::Error, fmt::Debug, future::Future, sync::Arc, time::Duration};
use tonic::{Code, Request};

/// List of gRPC error codes that client will retry.
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

/// A wrapper for a [WorkflowClientTrait] or [crate::WorkflowService] implementor which performs
/// auto-retries
#[derive(Debug, Clone)]
pub struct RetryClient<SG> {
    client: SG,
    retry_config: Arc<RetryConfig>,
}

impl<SG> RetryClient<SG> {
    /// Use the provided retry config with the provided client
    pub fn new(client: SG, retry_config: RetryConfig) -> Self {
        Self {
            client,
            retry_config: Arc::new(retry_config),
        }
    }
}

impl<SG> RetryClient<SG> {
    /// Return the inner client type
    pub fn get_client(&self) -> &SG {
        &self.client
    }

    /// Return the inner client type mutably
    pub fn get_client_mut(&mut self) -> &mut SG {
        &mut self.client
    }

    /// Disable retry and return the inner client type
    pub fn into_inner(self) -> SG {
        self.client
    }

    pub(crate) fn get_call_info<R>(
        &self,
        call_name: &'static str,
        request: Option<&Request<R>>,
    ) -> CallInfo {
        let mut call_type = CallType::Normal;
        let mut retry_short_circuit = None;
        if let Some(r) = request.as_ref() {
            let ext = r.extensions();
            if ext.get::<IsUserLongPoll>().is_some() {
                call_type = CallType::UserLongPoll;
            } else if ext.get::<IsWorkerTaskLongPoll>().is_some() {
                call_type = CallType::TaskLongPoll;
            }

            retry_short_circuit = ext.get::<NoRetryOnMatching>().cloned();
        }
        let retry_cfg = if call_type == CallType::TaskLongPoll {
            RetryConfig::task_poll_retry_policy()
        } else {
            (*self.retry_config).clone()
        };
        CallInfo {
            call_type,
            call_name,
            retry_cfg,
            retry_short_circuit,
        }
    }

    pub(crate) fn make_future_retry<R, F, Fut>(
        info: CallInfo,
        factory: F,
    ) -> FutureRetry<F, TonicErrorHandler<SystemClock>>
    where
        F: FnMut() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        FutureRetry::new(
            factory,
            TonicErrorHandler::new(info, RetryConfig::throttle_retry_policy()),
        )
    }
}

impl NamespacedClient for RetryClient<Client> {
    fn namespace(&self) -> &str {
        &self.client.namespace
    }

    fn get_identity(&self) -> &str {
        &self.client.options().identity
    }
}

#[derive(Debug)]
pub(crate) struct TonicErrorHandler<C: Clock> {
    backoff: ExponentialBackoff<C>,
    throttle_backoff: ExponentialBackoff<C>,
    max_retries: usize,
    call_type: CallType,
    call_name: &'static str,
    have_retried_goaway_cancel: bool,
    retry_short_circuit: Option<NoRetryOnMatching>,
}
impl TonicErrorHandler<SystemClock> {
    fn new(call_info: CallInfo, throttle_cfg: RetryConfig) -> Self {
        Self::new_with_clock(
            call_info,
            throttle_cfg,
            SystemClock::default(),
            SystemClock::default(),
        )
    }
}
impl<C> TonicErrorHandler<C>
where
    C: Clock,
{
    fn new_with_clock(
        call_info: CallInfo,
        throttle_cfg: RetryConfig,
        clock: C,
        throttle_clock: C,
    ) -> Self {
        Self {
            call_type: call_info.call_type,
            call_name: call_info.call_name,
            max_retries: call_info.retry_cfg.max_retries,
            backoff: call_info.retry_cfg.into_exp_backoff(clock),
            throttle_backoff: throttle_cfg.into_exp_backoff(throttle_clock),
            have_retried_goaway_cancel: false,
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
    retry_cfg: RetryConfig,
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

impl<C> ErrorHandler<tonic::Status> for TonicErrorHandler<C>
where
    C: Clock,
{
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

        // Sometimes we can get a GOAWAY that, for whatever reason, isn't quite properly handled
        // by hyper or some other internal lib, and we want to retry that still. We'll retry that
        // at most once. Ideally this bit should be removed eventually if we can repro the upstream
        // bug and it is fixed.
        let mut goaway_retry_allowed = false;
        if !self.have_retried_goaway_cancel
            && e.code() == Code::Cancelled
            && let Some(e) = e
                .source()
                .and_then(|e| e.downcast_ref::<tonic::transport::Error>())
                .and_then(|te| te.source())
                .and_then(|tec| tec.downcast_ref::<hyper::Error>())
            && format!("{e:?}").contains("connection closed")
        {
            goaway_retry_allowed = true;
            self.have_retried_goaway_cancel = true;
        }

        if RETRYABLE_ERROR_CODES.contains(&e.code()) || long_poll_allowed || goaway_retry_allowed {
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
            && self.backoff.get_elapsed_time() <= LONG_POLL_FATAL_GRACE
        {
            // We permit "fatal" errors while long polling for a while, because some proxies return
            // stupid error codes while getting ready, among other weird infra issues
            RetryPolicy::WaitRetry(self.backoff.max_interval)
        } else {
            RetryPolicy::ForwardError(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use backoff::Clock;
    use std::{ops::Add, time::Instant};
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueRequest, PollNexusTaskQueueRequest, PollWorkflowTaskQueueRequest,
    };
    use tonic::{IntoRequest, Status};

    /// Predefined retry configs with low durations to make unit tests faster
    const TEST_RETRY_CONFIG: RetryConfig = RetryConfig {
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

    struct FixedClock(Instant);
    impl Clock for FixedClock {
        fn now(&self) -> Instant {
            self.0
        }
    }

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
                let mut err_handler = TonicErrorHandler::new_with_clock(
                    CallInfo {
                        call_type: CallType::TaskLongPoll,
                        call_name,
                        retry_cfg: TEST_RETRY_CONFIG,
                        retry_short_circuit: None,
                    },
                    TEST_RETRY_CONFIG,
                    FixedClock(Instant::now()),
                    FixedClock(Instant::now()),
                );
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.backoff.clock.0 = err_handler
                    .backoff
                    .clock
                    .0
                    .add(LONG_POLL_FATAL_GRACE + Duration::from_secs(1));
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::ForwardError(_));
            }
        }
    }

    #[tokio::test]
    async fn long_poll_retryable_errors_never_fatal() {
        for code in RETRYABLE_ERROR_CODES {
            for call_name in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler::new_with_clock(
                    CallInfo {
                        call_type: CallType::TaskLongPoll,
                        call_name,
                        retry_cfg: TEST_RETRY_CONFIG,
                        retry_short_circuit: None,
                    },
                    TEST_RETRY_CONFIG,
                    FixedClock(Instant::now()),
                    FixedClock(Instant::now()),
                );
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.backoff.clock.0 = err_handler
                    .backoff
                    .clock
                    .0
                    .add(LONG_POLL_FATAL_GRACE + Duration::from_secs(1));
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }

    #[tokio::test]
    async fn retry_resource_exhausted() {
        let mut err_handler = TonicErrorHandler::new_with_clock(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            RetryConfig {
                initial_interval: Duration::from_millis(2),
                randomization_factor: 0.0,
                multiplier: 4.0,
                max_interval: Duration::from_millis(10),
                max_elapsed_time: None,
                max_retries: 10,
            },
            FixedClock(Instant::now()),
            FixedClock(Instant::now()),
        );
        let result = err_handler.handle(1, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(2)),
            _ => panic!(),
        }
        err_handler.backoff.clock.0 = err_handler.backoff.clock.0.add(Duration::from_millis(10));
        err_handler.throttle_backoff.clock.0 = err_handler
            .throttle_backoff
            .clock
            .0
            .add(Duration::from_millis(10));
        let result = err_handler.handle(2, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(8)),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn retry_short_circuit() {
        let mut err_handler = TonicErrorHandler::new_with_clock(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: Some(NoRetryOnMatching {
                    predicate: |s: &Status| s.code() == Code::ResourceExhausted,
                }),
            },
            TEST_RETRY_CONFIG,
            FixedClock(Instant::now()),
            FixedClock(Instant::now()),
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
        let mut err_handler = TonicErrorHandler::new_with_clock(
            CallInfo {
                call_type: CallType::TaskLongPoll,
                call_name: POLL_WORKFLOW_METH_NAME,
                retry_cfg: TEST_RETRY_CONFIG,
                retry_short_circuit: None,
            },
            TEST_RETRY_CONFIG,
            FixedClock(Instant::now()),
            FixedClock(Instant::now()),
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
        let fake_retry = RetryClient::new((), TEST_RETRY_CONFIG);
        let mut req = req.into_request();
        req.extensions_mut().insert(IsWorkerTaskLongPoll);
        for i in 1..=50 {
            let mut err_handler = TonicErrorHandler::new(
                fake_retry.get_call_info::<R>(call_name, Some(&req)),
                RetryConfig::throttle_retry_policy(),
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
        let fake_retry = RetryClient::new((), TEST_RETRY_CONFIG);
        let mut req = req.into_request();
        req.extensions_mut().insert(IsWorkerTaskLongPoll);
        // For some reason we will get cancelled in these situations occasionally (always?) too
        for code in [Code::Cancelled, Code::DeadlineExceeded] {
            let mut err_handler = TonicErrorHandler::new(
                fake_retry.get_call_info::<R>(call_name, Some(&req)),
                RetryConfig::throttle_retry_policy(),
            );
            for i in 1..=5 {
                let result = err_handler.handle(i, Status::new(code, "retryable failure"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }
}
