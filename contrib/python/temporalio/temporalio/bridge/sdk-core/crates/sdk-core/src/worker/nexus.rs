use crate::{
    abstractions::UsedMeteredSemPermit,
    pollers::{BoxedNexusPoller, NexusPollItem, new_nexus_task_poller},
    telemetry::metrics::{self, FailureReason, MetricsContext},
    worker::{CompleteNexusError, NexusSlotKind, PollError, client::WorkerClient},
};
use anyhow::anyhow;
use futures_util::{
    Stream, StreamExt, stream,
    stream::{BoxStream, PollNext},
};
use prost_types::Timestamp;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporalio_client::payload_limit_violation_from;
use temporalio_common::{
    payload_limits::PayloadLimitViolation,
    protos::{
        TaskToken,
        coresdk::{
            NexusSlotInfo,
            nexus::{
                CancelNexusTask, NexusOperationErrorState, NexusTask, NexusTaskCancelReason,
                nexus_task, nexus_task_completion,
            },
        },
        temporal::api::{
            enums::v1::NexusHandlerErrorRetryBehavior,
            failure::v1::failure::FailureInfo,
            nexus::{
                self,
                v1::{
                    NexusTaskFailure, UnsuccessfulOperationError, request::Variant, response,
                    start_operation_response,
                },
            },
        },
        utilities::normalize_http_headers,
    },
};
use tokio::{
    join,
    sync::{Mutex, Notify, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

static REQUEST_TIMEOUT_HEADER: &str = "request-timeout";

/// Centralizes all state related to received nexus tasks
pub(super) struct NexusManager {
    task_stream: Mutex<BoxStream<'static, Result<NexusTask, PollError>>>,
    /// Token to notify when poll returned a shutdown error
    poll_returned_shutdown_token: CancellationToken,
    /// Outstanding nexus tasks that have been issued to lang but not yet completed
    outstanding_task_map: OutstandingTaskMap,
    /// Notified every time a task in the map is completed
    task_completed_notify: Arc<Notify>,

    ever_polled: AtomicBool,
    metrics: MetricsContext,
}

impl NexusManager {
    pub(super) fn new(
        poller: BoxedNexusPoller,
        metrics: MetricsContext,
        graceful_shutdown: Option<Duration>,
        shutdown_initiated_token: CancellationToken,
    ) -> Self {
        let source_stream =
            new_nexus_task_poller(poller, metrics.clone(), shutdown_initiated_token);
        let (cancels_tx, cancels_rx) = tokio::sync::mpsc::unbounded_channel();
        let task_stream_input = stream::select_with_strategy(
            UnboundedReceiverStream::new(cancels_rx).map(TaskStreamInput::from),
            source_stream
                .map(|p| TaskStreamInput::from(Box::new(p)))
                .chain(stream::once(async move { TaskStreamInput::SourceComplete })),
            |_: &mut ()| PollNext::Left,
        );
        let task_completed_notify = Arc::new(Notify::new());
        let task_stream = NexusTaskStream::new(
            task_stream_input,
            cancels_tx,
            task_completed_notify.clone(),
            graceful_shutdown,
            metrics.clone(),
        );
        let outstanding_task_map = task_stream.outstanding_task_map.clone();
        Self {
            task_stream: Mutex::new(task_stream.into_stream().boxed()),
            poll_returned_shutdown_token: CancellationToken::new(),
            outstanding_task_map,
            task_completed_notify,
            ever_polled: AtomicBool::new(false),
            metrics,
        }
    }

    /// Block until then next nexus task is received from server
    pub(super) async fn next_nexus_task(&self) -> Result<NexusTask, PollError> {
        self.ever_polled.store(true, Ordering::Relaxed);
        let mut sl = self.task_stream.lock().await;
        let r = sl.next().await.unwrap_or_else(|| Err(PollError::ShutDown));
        // This can't happen in the or_else closure because ShutDown is typically returned by the
        // stream directly, before it terminates.
        if let Err(PollError::ShutDown) = &r {
            self.poll_returned_shutdown_token.cancel();
        }
        r
    }

    pub(super) async fn complete_task(
        &self,
        tt: TaskToken,
        status: nexus_task_completion::Status,
        client: &dyn WorkerClient,
    ) -> Result<(), CompleteNexusError> {
        let removed = self.outstanding_task_map.lock().remove(&tt);
        if let Some(task_info) = removed {
            self.metrics
                .nexus_task_execution_latency(task_info.start_time.elapsed());
            task_info.timeout_task.inspect(|jh| jh.abort());
            let (did_send, maybe_net_err) = match status {
                nexus_task_completion::Status::Completed(mut c) => {
                    // Server doesn't provide obvious errors for this validation, so it's done
                    // here to make life easier for lang implementors.
                    match &mut c.variant {
                        Some(response::Variant::StartOperation(so)) => {
                            #[allow(deprecated)]
                            if let Some(start_operation_response::Variant::OperationError(oe)) =
                                so.variant.as_ref()
                            {
                                // Deprecated branch left for SDKs that have not yet started using Temporal failures
                                self.metrics
                                    .with_new_attrs([metrics::failure_reason(
                                        FailureReason::NexusOperation(oe.operation_state.clone()),
                                    )])
                                    .nexus_task_execution_failed();
                            } else if let Some(start_operation_response::Variant::Failure(f)) =
                                &mut so.variant
                            {
                                let operation_state = match &f.failure_info {
                                    Some(FailureInfo::ApplicationFailureInfo(_)) => {
                                        NexusOperationErrorState::Failed
                                    }
                                    Some(FailureInfo::CanceledFailureInfo(_)) => {
                                        NexusOperationErrorState::Canceled
                                    }
                                    _ => {
                                        return Err(CompleteNexusError::MalformedNexusCompletion {
                                            reason: "Nexus StartOperationResponse with a failure must contain ApplicationFailureInfo or CanceledFailureInfo"
                                                .to_string(),
                                        });
                                    }
                                };

                                let use_temporal_failures = task_info
                                    .capabilities
                                    .as_ref()
                                    .map(|c| c.temporal_failure_responses)
                                    .unwrap_or_default();

                                if !use_temporal_failures {
                                    // Take the failure from the StartOperationResponse variant
                                    let failure = std::mem::take(f);

                                    // Convert the failure to an UnsuccessfulOperationError
                                    let failure =
                                        nexus::v1::Failure::try_from(failure)
                                        .map_err(|err| CompleteNexusError::MalformedNexusCompletion {
                                            reason: format!(
                                                "error converting temporal failure to nexus failure: {:?}",
                                                err
                                            ),
                                        })?;

                                    // Set StartOperationResponse variant to new UnsuccessfulOperationError
                                    so.variant = Some(
                                        #[allow(deprecated)]
                                        start_operation_response::Variant::OperationError(
                                            UnsuccessfulOperationError {
                                                operation_state: operation_state.to_string(),
                                                failure: Some(failure),
                                            },
                                        ),
                                    )
                                }

                                self.metrics
                                    .with_new_attrs([metrics::failure_reason(
                                        FailureReason::NexusOperation(operation_state.to_string()),
                                    )])
                                    .nexus_task_execution_failed();
                            }

                            if task_info.request_kind != RequestKind::Start {
                                return Err(CompleteNexusError::MalformedNexusCompletion {
                                    reason: "Nexus response was StartOperation but request was not"
                                        .to_string(),
                                });
                            }
                        }
                        Some(response::Variant::CancelOperation(_)) => {
                            if task_info.request_kind != RequestKind::Cancel {
                                return Err(CompleteNexusError::MalformedNexusCompletion {
                                    reason:
                                        "Nexus response was CancelOperation but request was not"
                                            .to_string(),
                                });
                            }
                        }
                        None => {
                            return Err(CompleteNexusError::MalformedNexusCompletion {
                                reason: "Nexus completion must contain a status variant "
                                    .to_string(),
                            });
                        }
                    }
                    let net_err = match client.complete_nexus_task(tt.clone(), c).await {
                        Ok(_) => None,
                        Err(e) => {
                            if let Some(violation) = payload_limit_violation_from(&e) {
                                client
                                    .fail_nexus_task(
                                        tt,
                                        payloads_too_large_nexus_failure(violation),
                                    )
                                    .await
                                    .err()
                            } else {
                                Some(e)
                            }
                        }
                    };
                    (true, net_err)
                }

                nexus_task_completion::Status::AckCancel(_) => {
                    self.metrics
                        .with_new_attrs([metrics::failure_reason(FailureReason::Timeout)])
                        .nexus_task_execution_failed();
                    (false, None)
                }

                #[allow(deprecated)]
                nexus_task_completion::Status::Error(e) => {
                    // Deprecated branch left for SDKs that have not yet started using Temporal failures
                    self.metrics
                        .with_new_attrs([metrics::failure_reason(
                            FailureReason::NexusHandlerError(e.error_type.clone()),
                        )])
                        .nexus_task_execution_failed();
                    let maybe_net_err = client
                        .fail_nexus_task(tt, NexusTaskFailure::Legacy(e))
                        .await
                        .err();
                    (true, maybe_net_err)
                }

                nexus_task_completion::Status::Failure(f) => {
                    let use_temporal_failures = task_info
                        .capabilities
                        .as_ref()
                        .map(|c| c.temporal_failure_responses)
                        .unwrap_or_default();

                    let failure_info = match &f.failure_info {
                        Some(FailureInfo::NexusHandlerFailureInfo(failure_info)) => {
                            failure_info.clone()
                        }
                        _ => {
                            return Err(CompleteNexusError::MalformedNexusCompletion {
                            reason: "Nexus completions with a failure must contain a NexusHandlerFailureInfo".to_string(),
                        });
                        }
                    };

                    self.metrics
                        .with_new_attrs([metrics::failure_reason(
                            FailureReason::NexusHandlerError(failure_info.r#type.clone()),
                        )])
                        .nexus_task_execution_failed();

                    let task_failure = if use_temporal_failures {
                        NexusTaskFailure::Temporal(f)
                    } else {
                        let failure = nexus::v1::Failure::try_from(f).map_err(|err| {
                            CompleteNexusError::MalformedNexusCompletion {
                                reason: format!(
                                    "error converting temporal failure to nexus failure: {:?}",
                                    err
                                ),
                            }
                        })?;

                        let h = nexus::v1::HandlerError {
                            error_type: failure_info.r#type,
                            failure: Some(failure),
                            //NexusHandlerFailureInfo and HandlerError both use enums::v1::NexusHandlerErrorRetryBehavior
                            retry_behavior: failure_info.retry_behavior,
                        };
                        NexusTaskFailure::Legacy(h)
                    };

                    let maybe_net_err = client.fail_nexus_task(tt, task_failure).await.err();
                    (true, maybe_net_err)
                }
            };

            self.task_completed_notify.notify_waiters();

            if let Some(e) = maybe_net_err {
                if e.code() == tonic::Code::NotFound {
                    warn!(details=?e, "Nexus task not found on completion. This \
                    may happen if the operation has already been cancelled but completed anyway.");
                } else {
                    warn!(error=?e, "Network error while completing Nexus task");
                }
            } else if did_send {
                // Record e2e latency if we sent replied to server without an RPC error
                if let Some(elapsed) = task_info.scheduled_time.and_then(|t| t.elapsed().ok()) {
                    self.metrics.nexus_task_e2e_latency(elapsed);
                }
            }
        } else {
            warn!(
                "Attempted to complete nexus task {} but we were not tracking it",
                &tt
            );
        }
        Ok(())
    }

    pub(super) async fn shutdown(&self) {
        if !self.ever_polled.load(Ordering::Relaxed) {
            return;
        }
        self.poll_returned_shutdown_token.cancelled().await;
    }
}

struct NexusTaskStream<S> {
    source_stream: S,
    outstanding_task_map: OutstandingTaskMap,
    cancels_tx: UnboundedSender<CancelNexusTask>,
    task_completed_notify: Arc<Notify>,
    grace_period: Option<Duration>,
    metrics: MetricsContext,
}

impl<S> NexusTaskStream<S>
where
    S: Stream<Item = TaskStreamInput>,
{
    fn new(
        source: S,
        cancels_tx: UnboundedSender<CancelNexusTask>,
        task_completed_notify: Arc<Notify>,
        grace_period: Option<Duration>,
        metrics: MetricsContext,
    ) -> Self {
        Self {
            source_stream: source,
            outstanding_task_map: Arc::new(Default::default()),
            cancels_tx,
            task_completed_notify,
            grace_period,
            metrics,
        }
    }

    fn into_stream(self) -> impl Stream<Item = Result<NexusTask, PollError>> {
        let outstanding_task_clone = self.outstanding_task_map.clone();
        let source_done = CancellationToken::new();
        let source_done_clone = source_done.clone();
        let cancels_tx_clone = self.cancels_tx.clone();
        self.source_stream
            .filter_map(move |t| {
                let res = match t {
                    TaskStreamInput::Poll(t) => match *t {
                        Ok(mut t) => {
                            if let Some(dur) = t.resp.sched_to_start() {
                                self.metrics.nexus_task_sched_to_start_latency(dur);
                            };

                            if let Some(ref mut req) = t.resp.request {
                                req.header = normalize_http_headers(std::mem::take(&mut req.header));
                                if let Some(nexus::v1::request::Variant::StartOperation(ref mut sor)) = req.variant {
                                    sor.callback_header = normalize_http_headers(std::mem::take(&mut sor.callback_header));
                                }
                            }

                            let tt = TaskToken(t.resp.task_token.clone());
                            let mut timeout_task = None;
                            let mut request_deadline: Option<Timestamp> = None;
                            if let Some(timeout_str) = t
                                .resp
                                .request
                                .as_ref()
                                .and_then(|r| r.header.get(REQUEST_TIMEOUT_HEADER))
                            {
                                if let Ok(timeout_dur) = parse_request_timeout(timeout_str) {
                                    request_deadline = Some((SystemTime::now() + timeout_dur).into());
                                    let tt_clone = tt.clone();
                                    let cancels_tx = self.cancels_tx.clone();
                                    timeout_task = Some(tokio::task::spawn(async move {
                                        tokio::time::sleep(timeout_dur).await;
                                        debug!(
                                        task_token=%tt_clone,
                                        "Timing out nexus task due to elapsed local timeout timer"
                                    );
                                        let _ = cancels_tx.send(CancelNexusTask {
                                            task_token: tt_clone.0,
                                            reason: NexusTaskCancelReason::TimedOut.into(),
                                        });
                                    }));
                                } else {
                                    // This could auto-respond and fail the nexus task, but given that
                                    // the server is going to try to parse this as well, and all we're
                                    // doing with this parsing is notifying the handler of a local
                                    // timeout, it seems reasonable to rely on server to handle this.
                                    warn!(
                                    "Failed to parse nexus timeout header value '{}'",
                                    timeout_str
                                );
                                }
                            }

                            let endpoint = t
                                .resp
                                .request
                                .as_ref()
                                .map(|r| r.endpoint.clone())
                                .unwrap_or_default();

                            let (service, operation, request_kind) = t
                                .resp
                                .request
                                .as_ref()
                                .and_then(|r| r.variant.as_ref())
                                .map(|v| match v {
                                    Variant::StartOperation(s) => (
                                        s.service.to_owned(),
                                        s.operation.to_owned(),
                                        RequestKind::Start,
                                    ),
                                    Variant::CancelOperation(c) => (
                                        c.service.to_owned(),
                                        c.operation.to_owned(),
                                        RequestKind::Cancel,
                                    ),
                                })
                                .unwrap_or_default();

                            let capabilities = t
                                .resp
                                .request
                                .as_ref()
                                .and_then(|r| r.capabilities);
                            self.outstanding_task_map.lock().insert(
                                tt,
                                NexusInFlightTask {
                                    request_kind,
                                    timeout_task,
                                    scheduled_time: t
                                        .resp
                                        .request
                                        .as_ref()
                                        .and_then(|r| r.scheduled_time)
                                        .and_then(|t| t.try_into().ok()),
                                    start_time: Instant::now(),
                                    _permit: t.permit.into_used(NexusSlotInfo { service, operation }),
                                    capabilities,
                                },
                            );
                            Some(Ok(NexusTask {
                                variant: Some(nexus_task::Variant::Task(t.resp)),
                                request_deadline,
                                endpoint,
                            }))
                        },
                        Err(e) => Some(Err(PollError::TonicError(e)))
                    },
                    TaskStreamInput::Cancel(c) => Some(Ok(NexusTask {
                        variant: Some(nexus_task::Variant::CancelTask(c)),
                        request_deadline: None,
                        endpoint: String::new(),
                    })),
                    TaskStreamInput::SourceComplete => {
                        source_done.cancel();
                        None
                    }
                };
                async move { res }
            })
            .take_until(async move {
                source_done_clone.cancelled().await;
                let (grace_killer, stop_grace) = futures_util::future::abortable(async {
                    if let Some(gp) = self.grace_period {
                        tokio::time::sleep(gp).await;
                        for (tt, _) in outstanding_task_clone.lock().iter() {
                            let _ = cancels_tx_clone.send(CancelNexusTask {
                                task_token: tt.0.clone(),
                                reason: NexusTaskCancelReason::WorkerShutdown.into(),
                            });
                        }
                    }
                });
                join!(
                    async {
                        while !outstanding_task_clone.lock().is_empty() {
                            self.task_completed_notify.notified().await;
                        }
                        // If we were waiting for the grace period but everything already finished,
                        // we don't need to keep waiting.
                        stop_grace.abort();
                    },
                    grace_killer
                )
            })
            .chain(stream::once(async move { Err(PollError::ShutDown) }))
    }
}

type OutstandingTaskMap = Arc<parking_lot::Mutex<HashMap<TaskToken, NexusInFlightTask>>>;

struct NexusInFlightTask {
    request_kind: RequestKind,
    timeout_task: Option<JoinHandle<()>>,
    scheduled_time: Option<SystemTime>,
    start_time: Instant,
    _permit: UsedMeteredSemPermit<NexusSlotKind>,
    capabilities: Option<nexus::v1::request::Capabilities>,
}

#[derive(Eq, PartialEq, Copy, Clone, Default)]
enum RequestKind {
    #[default]
    Start,
    Cancel,
}

#[derive(derive_more::From)]
enum TaskStreamInput {
    Poll(Box<NexusPollItem>),
    Cancel(CancelNexusTask),
    SourceComplete,
}

fn payloads_too_large_nexus_failure(violation: &PayloadLimitViolation) -> NexusTaskFailure {
    NexusTaskFailure::Legacy(nexus::v1::HandlerError {
        error_type: crate::worker::PAYLOADS_TOO_LARGE_FAILURE_TYPE.to_string(),
        failure: Some(nexus::v1::Failure {
            message: violation.to_string(),
            ..Default::default()
        }),
        retry_behavior: NexusHandlerErrorRetryBehavior::Retryable as i32,
    })
}

/// Parses the value of the nexus `request-timeout` header, which the Nexus spec defines as a
/// positive decimal followed by `ms`, `s`, or `m`.
///
/// Temporal server formats this header with Go's `time.Duration::String`, which is a superset of
/// that grammar: it also emits sub-millisecond units, hours, multi-unit values such as `1m30s`,
/// and negative values when the task's deadline has already elapsed by the time the task is
/// dispatched (see <https://github.com/temporalio/temporal/issues/11569>). All of those are accepted
/// here, and anything that resolves to a non-positive duration means the deadline is already past,
/// hence a zero timeout.
fn parse_request_timeout(timeout: &str) -> Result<Duration, anyhow::Error> {
    let trimmed = timeout.trim();
    let (negative, mut rest) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    if rest.is_empty() {
        return Err(anyhow!("Invalid timeout format"));
    }

    let mut total = Duration::ZERO;
    while !rest.is_empty() {
        let unit_start = rest
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .ok_or_else(|| anyhow!("Invalid timeout format"))?;
        let (value, after_value) = rest.split_at(unit_start);
        let unit_end = after_value
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(after_value.len());
        let (unit, remainder) = after_value.split_at(unit_end);

        let value = value.parse::<f64>()?;
        let seconds = match unit {
            "ns" => value / 1e9,
            // Go emits the micro sign, but tolerate the Greek letter and the ASCII form too.
            "us" | "µs" | "μs" => value / 1e6,
            "ms" => value / 1e3,
            "s" => value,
            "m" => value * 60.0,
            "h" => value * 3600.0,
            _ => return Err(anyhow!("Invalid timeout format")),
        };
        total += Duration::try_from_secs_f64(seconds)?;
        rest = remainder;
    }

    Ok(if negative { Duration::ZERO } else { total })
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::payload_limits::{LimitClass, LimitSeverity};

    #[tokio::test]
    async fn test_parse_request_timeout() {
        assert_eq!(
            parse_request_timeout("10m").unwrap(),
            Duration::from_secs(600)
        );
        assert_eq!(
            parse_request_timeout("30s").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_request_timeout("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_request_timeout("9.155416ms").unwrap(),
            Duration::from_secs_f64(9.155416 / 1000.0)
        );
    }

    #[test]
    fn parse_request_timeout_go_duration_forms() {
        // Units outside the Nexus grammar which Go's `time.Duration::String` may still emit
        assert_eq!(
            parse_request_timeout("88.458µs").unwrap(),
            Duration::from_secs_f64(88.458 / 1e6)
        );
        assert_eq!(
            parse_request_timeout("125ns").unwrap(),
            Duration::from_nanos(125)
        );
        assert_eq!(
            parse_request_timeout("2h").unwrap(),
            Duration::from_secs(7200)
        );
        // Multi-unit values
        assert_eq!(
            parse_request_timeout("1m30.5s").unwrap(),
            Duration::from_secs_f64(90.5)
        );
        assert_eq!(
            parse_request_timeout("1h0m0s").unwrap(),
            Duration::from_secs(3600)
        );
        // An already-elapsed deadline means the task must be timed out right away
        assert_eq!(parse_request_timeout("-88.458µs").unwrap(), Duration::ZERO);
        assert_eq!(parse_request_timeout("-1m30s").unwrap(), Duration::ZERO);
        assert_eq!(parse_request_timeout("0s").unwrap(), Duration::ZERO);
        // Leading and surrounding whitespace, and an explicit positive sign
        assert_eq!(
            parse_request_timeout("  +10s  ").unwrap(),
            Duration::from_secs(10)
        );

        for invalid in ["", "-", "10", "10x", "abc", "s", "1s2"] {
            assert!(
                parse_request_timeout(invalid).is_err(),
                "'{invalid}' should not parse"
            );
        }
    }

    #[test]
    fn payloads_too_large_nexus_failure_is_retryable() {
        let violation = PayloadLimitViolation {
            path: "payload".to_string(),
            class: LimitClass::Blob,
            severity: LimitSeverity::Error,
            size: 1024,
            limit: 10,
        };
        let NexusTaskFailure::Legacy(handler_err) = payloads_too_large_nexus_failure(&violation)
        else {
            panic!("expected a legacy handler error");
        };
        assert_eq!(
            handler_err.error_type,
            crate::worker::PAYLOADS_TOO_LARGE_FAILURE_TYPE
        );
        assert_eq!(
            handler_err.retry_behavior,
            NexusHandlerErrorRetryBehavior::Retryable as i32
        );
    }
}
