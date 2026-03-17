use crate::{
    abstractions::UsedMeteredSemPermit,
    pollers::{BoxedNexusPoller, NexusPollItem, new_nexus_task_poller},
    telemetry::{
        metrics,
        metrics::{FailureReason, MetricsContext},
    },
    worker::client::WorkerClient,
};
use anyhow::anyhow;
use futures_util::{
    Stream, StreamExt, stream,
    stream::{BoxStream, PollNext},
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_api::{
    errors::{CompleteNexusError, PollError},
    worker::NexusSlotKind,
};
use temporal_sdk_core_protos::{
    TaskToken,
    coresdk::{
        NexusSlotInfo,
        nexus::{
            CancelNexusTask, NexusTask, NexusTaskCancelReason, nexus_task, nexus_task_completion,
        },
    },
    temporal::api::nexus::v1::{request::Variant, response, start_operation_response},
};
use tokio::{
    join,
    sync::{Mutex, Notify, mpsc::UnboundedSender},
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

static REQUEST_TIMEOUT_HEADER: &str = "Request-Timeout";

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
                nexus_task_completion::Status::Completed(c) => {
                    // Server doesn't provide obvious errors for this validation, so it's done
                    // here to make life easier for lang implementors.
                    match &c.variant {
                        Some(response::Variant::StartOperation(so)) => {
                            if let Some(start_operation_response::Variant::OperationError(oe)) =
                                so.variant.as_ref()
                            {
                                self.metrics
                                    .with_new_attrs([metrics::failure_reason(
                                        FailureReason::NexusOperation(oe.operation_state.clone()),
                                    )])
                                    .nexus_task_execution_failed();
                            };
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
                    (true, client.complete_nexus_task(tt, c).await.err())
                }
                nexus_task_completion::Status::AckCancel(_) => {
                    self.metrics
                        .with_new_attrs([metrics::failure_reason(FailureReason::Timeout)])
                        .nexus_task_execution_failed();
                    (false, None)
                }
                nexus_task_completion::Status::Error(e) => {
                    self.metrics
                        .with_new_attrs([metrics::failure_reason(
                            FailureReason::NexusHandlerError(e.error_type.clone()),
                        )])
                        .nexus_task_execution_failed();
                    (true, client.fail_nexus_task(tt, e).await.err())
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
                        Ok(t) => {
                            if let Some(dur) = t.resp.sched_to_start() {
                                self.metrics.nexus_task_sched_to_start_latency(dur);
                            };

                            let tt = TaskToken(t.resp.task_token.clone());
                            let mut timeout_task = None;
                            if let Some(timeout_str) = t
                                .resp
                                .request
                                .as_ref()
                                .and_then(|r| r.header.get(REQUEST_TIMEOUT_HEADER))
                            {
                                if let Ok(timeout_dur) = parse_request_timeout(timeout_str) {
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
                                },
                            );
                            Some(Ok(NexusTask {
                                variant: Some(nexus_task::Variant::Task(t.resp)),
                            }))
                        },
                        Err(e) => Some(Err(PollError::TonicError(e)))
                    },
                    TaskStreamInput::Cancel(c) => Some(Ok(NexusTask {
                        variant: Some(nexus_task::Variant::CancelTask(c)),
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

fn parse_request_timeout(timeout: &str) -> Result<Duration, anyhow::Error> {
    let timeout = timeout.trim();
    let (value, unit) = timeout.split_at(
        timeout
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(timeout.len()),
    );

    match unit {
        "m" => value
            .parse::<f64>()
            .map(|v| Duration::from_secs_f64(60.0 * v))
            .map_err(Into::into),
        "s" => value
            .parse::<f64>()
            .map(Duration::from_secs_f64)
            .map_err(Into::into),
        "ms" => value
            .parse::<f64>()
            .map_err(anyhow::Error::from)
            .and_then(|v| Duration::try_from_secs_f64(v / 1000.0).map_err(Into::into)),
        _ => Err(anyhow!("Invalid timeout format")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
