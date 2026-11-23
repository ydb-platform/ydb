mod activity_heartbeat_manager;
mod local_activities;

pub(crate) use local_activities::{
    ExecutingLAId, LACompleteAction, LocalActRequest, LocalActivityExecutionResult,
    LocalActivityManager, LocalActivityResolution, NewLocalAct, NextPendingLAAction,
};

use crate::{
    TaskToken,
    abstractions::{
        ClosableMeteredPermitDealer, MeteredPermitDealer, TrackedOwnedMeteredSemPermit,
        UsedMeteredSemPermit,
    },
    pollers::{BoxedActPoller, PermittedTqResp, TrackedPermittedTqResp, new_activity_task_poller},
    telemetry::metrics::{
        MetricsContext, activity_type, eager, should_record_failure_metric, workflow_type,
    },
    worker::{
        ActivitySlotKind, PollError,
        activities::activity_heartbeat_manager::ActivityHeartbeatError, client::WorkerClient,
    },
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use futures_util::{
    Stream, StreamExt, stream,
    stream::{BoxStream, PollNext},
};
use std::{
    collections::HashMap,
    convert::TryInto,
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporalio_client::{payload_limit_violation_from, worker::CancelActivityCallback};
use temporalio_common::{
    payload_limits::PayloadLimitViolation,
    protos::{
        coresdk::{
            ActivityHeartbeat, ActivitySlotInfo,
            activity_result::{self as ar, activity_execution_result as aer},
            activity_task::{ActivityCancelReason, ActivityCancellationDetails, ActivityTask},
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            failure::v1::{
                ApplicationFailureInfo, CanceledFailureInfo, Failure, failure::FailureInfo,
            },
            workflowservice::v1::PollActivityTaskQueueResponse,
        },
    },
};
use tokio::{
    join,
    sync::{
        Mutex, Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::Span;

type OutstandingActMap = Arc<parking_lot::Mutex<HashMap<TaskToken, RemoteInFlightActInfo>>>;

#[derive(Debug)]
struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
    details: ActivityCancellationDetails,
}

impl PendingActivityCancel {
    fn new(
        task_token: TaskToken,
        reason: ActivityCancelReason,
        details: ActivityCancellationDetails,
    ) -> Self {
        Self {
            task_token,
            reason,
            details,
        }
    }
}

/// Contains details that core wants to store while an activity is running.
#[derive(Debug)]
struct InFlightActInfo {
    activity_type: String,
    workflow_type: String,
    /// Only kept for logging reasons
    workflow_id: String,
    /// Only kept for logging reasons
    workflow_run_id: String,
    start_time: Instant,
    scheduled_time: Option<SystemTime>,
}

/// Augments [InFlightActInfo] with details specific to remote activities
struct RemoteInFlightActInfo {
    base: InFlightActInfo,
    /// Used to calculate aggregation delay between activity heartbeats.
    heartbeat_timeout: Option<prost_types::Duration>,
    /// Set if we have already issued a cancellation activation to lang for this activity, with
    /// the original reason we issued the cancel.
    issued_cancel_to_lang: Option<ActivityCancelReason>,
    /// Set to true if we have already learned from the server this activity doesn't exist. EX:
    /// we have learned from heartbeating and issued a cancel task, in which case we may simply
    /// discard the reply.
    known_not_found: bool,
    /// Handle to the task containing local timeout tracking, if any.
    local_timeouts_task: Option<JoinHandle<()>>,
    /// Used to reset the local heartbeat timeout every time we record a heartbeat
    timeout_resetter: Option<Arc<Notify>>,
    /// The most recent heartbeat received from lang, independently of whether it has been sent.
    last_heartbeat_details: Option<Vec<Payload>>,
    /// The permit from the max concurrent semaphore
    _permit: UsedMeteredSemPermit<ActivitySlotKind>,
}
impl RemoteInFlightActInfo {
    fn new(
        poll_resp: &PollActivityTaskQueueResponse,
        permit: UsedMeteredSemPermit<ActivitySlotKind>,
    ) -> Self {
        let wec = poll_resp.workflow_execution.clone().unwrap_or_default();
        Self {
            base: InFlightActInfo {
                activity_type: poll_resp.activity_type.clone().unwrap_or_default().name,
                workflow_type: poll_resp.workflow_type.clone().unwrap_or_default().name,
                workflow_id: wec.workflow_id,
                workflow_run_id: wec.run_id,
                start_time: Instant::now(),
                scheduled_time: poll_resp.scheduled_time.and_then(|i| i.try_into().ok()),
            },
            heartbeat_timeout: poll_resp.heartbeat_timeout,
            issued_cancel_to_lang: None,
            known_not_found: false,
            local_timeouts_task: None,
            timeout_resetter: None,
            last_heartbeat_details: None,
            _permit: permit,
        }
    }
}

pub(crate) struct WorkerActivityTasks {
    /// Token which is cancelled once shutdown is beginning
    shutdown_initiated_token: CancellationToken,
    /// Centralizes management of heartbeat issuing / throttling
    heartbeat_manager: ActivityHeartbeatManager,
    /// Combined stream for any ActivityTask producing source (polls, eager activities,
    /// cancellations)
    activity_task_stream: Mutex<BoxStream<'static, Result<ActivityTask, PollError>>>,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: OutstandingActMap,
    /// Ensures we don't exceed this worker's maximum concurrent activity limit for activities. This
    /// semaphore is used to limit eager activities but shares the same underlying
    /// [MeteredPermitDealer] that is used to limit the concurrency for non-eager activities.
    eager_activities_semaphore: Arc<ClosableMeteredPermitDealer<ActivitySlotKind>>,
    /// Holds activity tasks we have received in direct response to workflow task completion (a.k.a
    /// eager activities). Tasks received in this stream hold a "tracked" permit that is issued by
    /// the `eager_activities_semaphore`.
    eager_activities_tx: UnboundedSender<TrackedPermittedTqResp<PollActivityTaskQueueResponse>>,
    /// Ensures that no activities are in the middle of flushing their results to server while we
    /// try to shut down.
    completers_lock: tokio::sync::RwLock<()>,

    metrics: MetricsContext,

    max_heartbeat_throttle_interval: Duration,
    default_heartbeat_throttle_interval: Duration,

    /// Wakes every time an activity is removed from the outstanding map
    complete_notify: Arc<Notify>,
    /// Token to notify when poll returned a shutdown error
    poll_returned_shutdown_token: CancellationToken,
    /// Used to inject external cancellations (e.g. from nexus worker commands)
    cancels_tx: UnboundedSender<PendingActivityCancel>,
}

#[derive(derive_more::From)]
enum ActivityTaskSource {
    PendingCancel(PendingActivityCancel),
    PendingStart(Box<Result<(PermittedTqResp<PollActivityTaskQueueResponse>, bool), PollError>>),
}

impl WorkerActivityTasks {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        semaphore: MeteredPermitDealer<ActivitySlotKind>,
        poller: BoxedActPoller,
        client: Arc<dyn WorkerClient>,
        metrics: MetricsContext,
        max_heartbeat_throttle_interval: Duration,
        default_heartbeat_throttle_interval: Duration,
        graceful_shutdown: Option<Duration>,
        local_timeout_buffer: Duration,
    ) -> Self {
        let shutdown_initiated_token = CancellationToken::new();
        let outstanding_activity_tasks = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let server_poller_stream =
            new_activity_task_poller(poller, metrics.clone(), shutdown_initiated_token.clone());
        let (eager_activities_tx, eager_activities_rx) = unbounded_channel();
        let eager_activities_semaphore = ClosableMeteredPermitDealer::new_arc(Arc::new(semaphore));

        let start_tasks_stream_complete = CancellationToken::new();
        let starts_stream = Self::merge_start_task_sources(
            eager_activities_rx,
            server_poller_stream,
            eager_activities_semaphore.clone(),
            start_tasks_stream_complete.clone(),
        );
        let (cancels_tx, cancels_rx) = unbounded_channel();
        let external_cancels_tx = cancels_tx.clone();
        let heartbeat_manager = ActivityHeartbeatManager::new(client, cancels_tx.clone());
        let complete_notify = Arc::new(Notify::new());
        let source_stream = stream::select_with_strategy(
            UnboundedReceiverStream::new(cancels_rx).map(ActivityTaskSource::from),
            starts_stream.map(|a| ActivityTaskSource::from(Box::new(a))),
            |_: &mut ()| PollNext::Left,
        );

        let activity_task_stream = ActivityTaskStream {
            source_stream,
            outstanding_tasks: outstanding_activity_tasks.clone(),
            start_tasks_stream_complete,
            complete_notify: complete_notify.clone(),
            grace_period: graceful_shutdown,
            cancels_tx,
            local_timeout_buffer,
            shutdown_initiated_token: shutdown_initiated_token.clone(),
            metrics: metrics.clone(),
        }
        .streamify();

        Self {
            shutdown_initiated_token,
            eager_activities_tx,
            heartbeat_manager,
            activity_task_stream: Mutex::new(activity_task_stream.boxed()),
            eager_activities_semaphore,
            complete_notify,
            metrics,
            max_heartbeat_throttle_interval,
            default_heartbeat_throttle_interval,
            poll_returned_shutdown_token: CancellationToken::new(),
            outstanding_activity_tasks,
            completers_lock: Default::default(),
            cancels_tx: external_cancels_tx,
        }
    }

    /// Merges the server poll and eager [ActivityTask] sources
    fn merge_start_task_sources(
        non_poll_tasks_rx: UnboundedReceiver<TrackedPermittedTqResp<PollActivityTaskQueueResponse>>,
        poller_stream: impl Stream<
            Item = Result<PermittedTqResp<PollActivityTaskQueueResponse>, tonic::Status>,
        >,
        eager_activities_semaphore: Arc<ClosableMeteredPermitDealer<ActivitySlotKind>>,
        on_complete_token: CancellationToken,
    ) -> impl Stream<Item = Result<(PermittedTqResp<PollActivityTaskQueueResponse>, bool), PollError>>
    {
        let non_poll_stream = stream::unfold(
            (non_poll_tasks_rx, eager_activities_semaphore),
            |(mut non_poll_tasks_rx, eager_activities_semaphore)| async move {
                loop {
                    tokio::select! {
                        biased;

                        task_opt = non_poll_tasks_rx.recv() => {
                            // Add is_eager true and wrap in Result
                            return task_opt.map(|task| (
                                Ok((PermittedTqResp{ permit: task.permit.into(), resp: task.resp },
                                    true)),
                                (non_poll_tasks_rx, eager_activities_semaphore)));
                        }
                        _ = eager_activities_semaphore.close_complete() => {
                            // Once shutting down, we stop accepting eager activities
                            non_poll_tasks_rx.close();
                            continue;
                        }
                    }
                }
            },
        );
        // Add is_eager false
        let poller_stream = poller_stream.map(|res| res.map(|task| (task, false)));

        // Prefer eager activities over polling the server
        stream::select_with_strategy(non_poll_stream, poller_stream, |_: &mut ()| PollNext::Left)
            .map(|res| Some(res.map_err(Into::into)))
            .chain(futures_util::stream::once(async move {
                on_complete_token.cancel();
                None
            }))
            .filter_map(future::ready)
    }

    pub(crate) fn initiate_shutdown(&self) {
        self.shutdown_initiated_token.cancel();
        self.eager_activities_semaphore.close();
    }

    pub(crate) async fn shutdown(&self) {
        self.initiate_shutdown();
        let _ = self.completers_lock.write().await;
        self.poll_returned_shutdown_token.cancelled().await;
        self.heartbeat_manager.shutdown().await;
    }

    /// Exclusive poll for activity tasks
    ///
    /// Polls the various task sources (server polls, eager activities, cancellations) while
    /// respecting the provided rate limits and allowed concurrency. Returns
    /// [PollError::ShutDown] after shutdown is completed and all tasks sources are
    /// depleted.
    pub(crate) async fn poll(&self) -> Result<ActivityTask, PollError> {
        let mut poller_stream = self.activity_task_stream.lock().await;
        poller_stream.next().await.unwrap_or_else(|| {
            self.poll_returned_shutdown_token.cancel();
            Err(PollError::ShutDown)
        })
    }

    pub(crate) async fn complete(
        &self,
        task_token: TaskToken,
        status: aer::Status,
        client: &dyn WorkerClient,
    ) {
        let act_info = {
            let mut outstanding_activity_tasks = self.outstanding_activity_tasks.lock();
            outstanding_activity_tasks.remove(&task_token)
        };
        if let Some(act_info) = act_info {
            let act_metrics = self.metrics.with_new_attrs([
                activity_type(act_info.base.activity_type),
                workflow_type(act_info.base.workflow_type),
            ]);
            Span::current().record("workflow_id", act_info.base.workflow_id);
            Span::current().record("run_id", act_info.base.workflow_run_id);
            act_metrics.act_execution_latency(act_info.base.start_time.elapsed());
            let known_not_found = act_info.known_not_found;

            if let Some(jh) = act_info.local_timeouts_task {
                jh.abort()
            };
            // Cancellation responses cannot carry heartbeat details, so normal cancellations must
            // flush them separately. Worker-shutdown cancellations are reported as failures below.
            let should_flush = !known_not_found
                && matches!(&status, aer::Status::Cancelled(_))
                && !matches!(
                    act_info.issued_cancel_to_lang,
                    Some(ActivityCancelReason::WorkerShutdown)
                );
            self.heartbeat_manager
                .evict(task_token.clone(), should_flush)
                .await;

            let last_heartbeat_details = act_info
                .last_heartbeat_details
                .map(|payloads| Payloads { payloads });

            // No need to report activities which we already know the server doesn't care about
            if !known_not_found {
                let _flushing_guard = self.completers_lock.read().await;
                let maybe_net_err = match status {
                    aer::Status::WillCompleteAsync(_) => None,
                    aer::Status::Completed(ar::Success { result }) => {
                        // If the gRPC layer rejects an oversized result, report the activity task as failed.
                        match client
                            .complete_activity_task(task_token.clone(), result.map(Into::into))
                            .await
                        {
                            Ok(_) => {
                                if let Some(sched_time) = act_info
                                    .base
                                    .scheduled_time
                                    .and_then(|st| st.elapsed().ok())
                                {
                                    act_metrics.act_execution_succeeded(sched_time);
                                }
                                None
                            }
                            Err(e) => {
                                if let Some(violation) = payload_limit_violation_from(&e) {
                                    act_metrics.act_execution_failed();
                                    client
                                        .fail_activity_task(
                                            task_token.clone(),
                                            Some(make_payloads_too_large_failure(violation)),
                                            last_heartbeat_details.clone(),
                                        )
                                        .await
                                        .err()
                                } else {
                                    Some(e)
                                }
                            }
                        }
                    }
                    aer::Status::Failed(ar::Failure { failure }) => {
                        if should_record_failure_metric(&failure) {
                            act_metrics.act_execution_failed();
                        }
                        client
                            .fail_activity_task(task_token.clone(), failure, last_heartbeat_details)
                            .await
                            .err()
                    }
                    aer::Status::Cancelled(ar::Cancellation { failure }) => {
                        if matches!(
                            act_info.issued_cancel_to_lang,
                            Some(ActivityCancelReason::WorkerShutdown),
                        ) {
                            // We report cancels for graceful shutdown as failures, so we
                            // don't wait for the whole timeout to elapse, which is what would
                            // happen anyway.
                            client
                                .fail_activity_task(
                                    task_token.clone(),
                                    Some(worker_shutdown_failure()),
                                    last_heartbeat_details,
                                )
                                .await
                                .err()
                        } else {
                            let details = if let Some(Failure {
                                failure_info:
                                    Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                                        details,
                                        ..
                                    })),
                                ..
                            }) = failure
                            {
                                details
                            } else {
                                warn!(task_token=?task_token,
                                    "Expected activity cancelled status with CanceledFailureInfo");
                                None
                            };
                            match client
                                .cancel_activity_task(task_token.clone(), details)
                                .await
                            {
                                Ok(_) => None,
                                Err(e) => {
                                    if let Some(violation) = payload_limit_violation_from(&e) {
                                        act_metrics.act_execution_failed();
                                        client
                                            .fail_activity_task(
                                                task_token.clone(),
                                                Some(make_payloads_too_large_failure(violation)),
                                                last_heartbeat_details,
                                            )
                                            .await
                                            .err()
                                    } else {
                                        Some(e)
                                    }
                                }
                            }
                        }
                    }
                };

                if let Some(e) = maybe_net_err {
                    if e.code() == tonic::Code::NotFound {
                        warn!(task_token=?task_token, details=?e, "Activity not found on \
                        completion. This may happen if the activity has already been cancelled but \
                        completed anyway.");
                    } else {
                        warn!(error=?e, "Network error while completing activity");
                    };
                };
            };
        } else {
            warn!(
                "Attempted to complete activity task {} but we were not tracking it",
                &task_token
            );
        }

        self.complete_notify.notify_waiters();
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(
        &self,
        details: ActivityHeartbeat,
    ) -> Result<(), ActivityHeartbeatError> {
        // TODO: Propagate these back as cancels. Silent fails is too nonobvious
        let (heartbeat_timeout, timeout_resetter) = {
            let mut outstanding_activity_tasks = self.outstanding_activity_tasks.lock();
            let at_info = outstanding_activity_tasks
                .get_mut(&TaskToken(details.task_token.clone()))
                .ok_or(ActivityHeartbeatError::UnknownActivity)?;
            at_info.last_heartbeat_details = Some(details.details.clone());
            (at_info.heartbeat_timeout, at_info.timeout_resetter.clone())
        };
        let heartbeat_timeout: Duration = heartbeat_timeout
            // We treat None as 0 (even though heartbeat_timeout is never set to None by the server)
            .unwrap_or_default()
            .try_into()
            // This technically should never happen since prost duration should be directly mappable
            // to std::time::Duration.
            .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;

        // There is a bug in the server that translates non-set heartbeat timeouts into 0 duration.
        // That's why we treat 0 the same way as None, otherwise we wouldn't know which aggregation
        // delay to use, and using 0 is not a good idea as SDK would hammer the server too hard.
        let throttle_interval = if heartbeat_timeout.as_millis() == 0 {
            self.default_heartbeat_throttle_interval
        } else {
            heartbeat_timeout.mul_f64(0.8)
        };
        let throttle_interval =
            std::cmp::min(throttle_interval, self.max_heartbeat_throttle_interval);
        self.heartbeat_manager
            .record(details, throttle_interval, timeout_resetter)
    }

    /// Returns a handle that the workflows management side can use to interact with this manager
    pub(crate) fn get_handle_for_workflows(&self) -> ActivitiesFromWFTsHandle {
        ActivitiesFromWFTsHandle {
            sem: self.eager_activities_semaphore.clone(),
            tx: self.eager_activities_tx.clone(),
        }
    }

    /// Returns a callback that can be used to cancel activities from outside this manager.
    pub(crate) fn cancel_activity_callback(&self) -> CancelActivityCallback {
        let outstanding = self.outstanding_activity_tasks.clone();
        let cancels_tx = self.cancels_tx.clone();
        Arc::new(move |task_token: TaskToken| {
            if outstanding.lock().contains_key(&task_token) {
                let _ = cancels_tx.send(PendingActivityCancel::new(
                    task_token,
                    ActivityCancelReason::Cancelled,
                    ActivityCancellationDetails {
                        is_cancelled: true,
                        ..Default::default()
                    },
                ));
                true
            } else {
                false
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.eager_activities_semaphore.unused_permits()
    }
}

struct ActivityTaskStream<SrcStrm> {
    source_stream: SrcStrm,
    outstanding_tasks: OutstandingActMap,
    start_tasks_stream_complete: CancellationToken,
    complete_notify: Arc<Notify>,
    grace_period: Option<Duration>,
    cancels_tx: UnboundedSender<PendingActivityCancel>,
    /// The extra time we'll wait for local timeouts before firing them, to avoid racing with server
    local_timeout_buffer: Duration,
    /// Token which is cancelled once shutdown is beginning
    shutdown_initiated_token: CancellationToken,
    metrics: MetricsContext,
}

impl<SrcStrm> ActivityTaskStream<SrcStrm>
where
    SrcStrm: Stream<Item = ActivityTaskSource>,
{
    /// Create a task stream composed of (in poll preference order):
    ///  cancels_stream ------------------------------+--- activity_task_stream
    ///  eager_activities_rx ---+--- starts_stream ---|
    ///  server_poll_stream  ---|
    fn streamify(self) -> impl Stream<Item = Result<ActivityTask, PollError>> {
        let outstanding_tasks_clone = self.outstanding_tasks.clone();
        let should_issue_immediate_cancel = Arc::new(AtomicBool::new(false));
        let should_issue_immediate_cancel_clone = should_issue_immediate_cancel.clone();
        let cancels_tx = self.cancels_tx.clone();
        self.source_stream
            .filter_map(move |source| {
                let res = match source {
                    ActivityTaskSource::PendingCancel(next_pc) => {
                        // It's possible that activity has been completed and we no longer have
                        // an outstanding activity task. This is fine because it means that we
                        // no longer need to cancel this activity, so we'll just ignore such
                        // orphaned cancellations.
                        {
                            let mut outstanding_tasks = self.outstanding_tasks.lock();
                            if let Some(details) = outstanding_tasks.get_mut(&next_pc.task_token) {
                                if details.issued_cancel_to_lang.is_some() {
                                    // Don't double-issue cancellations
                                    None
                                } else {
                                    details.issued_cancel_to_lang = Some(next_pc.reason);
                                    if next_pc.reason == ActivityCancelReason::NotFound
                                        || next_pc.details.is_not_found
                                    {
                                        details.known_not_found = true;
                                    }
                                    Some(Ok(ActivityTask::cancel_from_ids(
                                        next_pc.task_token.0,
                                        next_pc.reason,
                                        next_pc.details,
                                    )))
                                }
                            } else {
                                debug!(task_token = %next_pc.task_token,
                                       "Unknown activity task when issuing cancel");
                                // If we can't find the activity here, it's already been completed,
                                // in which case issuing a cancel again is pointless.
                                None
                            }
                        }
                    }
                    ActivityTaskSource::PendingStart(res) => {
                        Some(res.map(|(task, is_eager)| {
                            let mut activity_type_name = "";
                            if let Some(ref act_type) = task.resp.activity_type {
                                activity_type_name = act_type.name.as_str();
                                if let Some(ref wf_type) = task.resp.workflow_type {
                                    self.metrics
                                        .with_new_attrs([
                                            activity_type(activity_type_name.to_owned()),
                                            workflow_type(wf_type.name.clone()),
                                            eager(is_eager),
                                        ])
                                        .act_task_received();
                                }
                            }
                            // There could be an else statement here but since the response
                            // should always contain both activity_type and workflow_type, we
                            // won't bother.

                            if let Some(dur) = task.resp.sched_to_start() {
                                self.metrics.act_sched_to_start_latency(dur);
                            };

                            let tt: TaskToken = task.resp.task_token.clone().into();
                            self.outstanding_tasks.lock().insert(
                                tt.clone(),
                                RemoteInFlightActInfo::new(
                                    &task.resp,
                                    task.permit.into_used(ActivitySlotInfo {
                                        activity_type: activity_type_name.to_string(),
                                    }),
                                ),
                            );

                            // If we have already waited the grace period and issued cancels,
                            // this will have been set true, indicating anything that happened
                            // to be buffered/in-flight/etc should get an immediate cancel. This
                            // is to allow the user to potentially decide to ignore cancels and
                            // do work on polls that got received during shutdown.
                            if should_issue_immediate_cancel.load(Ordering::Acquire) {
                                let _ = cancels_tx.send(PendingActivityCancel::new(
                                    tt.clone(),
                                    ActivityCancelReason::WorkerShutdown,
                                    ActivityTask::primary_reason_to_cancellation_details(
                                        ActivityCancelReason::WorkerShutdown,
                                    ),
                                ));
                            } else {
                                // Fire off task to keep track of local timeouts. We do this so that
                                // activities can still get cleaned up even if the user isn't
                                // heartbeating. Schedule to closed is not tracked due to the
                                // possibility of clock skew messing things up, and it's relative
                                // unlikeliness compared to the other timeouts.
                                if let Some(timers) = ActivityLocalTimers::new(
                                    task.resp.heartbeat_timeout,
                                    task.resp.start_to_close_timeout,
                                    self.local_timeout_buffer,
                                ) {
                                    let resetter = timers.heartbeat_resetter();
                                    let cancel_tx = cancels_tx.clone();
                                    let task_token = tt.clone();
                                    let local_timeouts_task =
                                        Some(tokio::task::spawn(async move {
                                            let timeout_type = timers.run().await;
                                            debug!(
                                                task_token=%task_token,
                                                "Timing out activity due to elapsed local {timeout_type} timer",
                                            );
                                            let _ = cancel_tx.send(PendingActivityCancel::new(
                                                task_token,
                                                ActivityCancelReason::TimedOut,
                                                ActivityCancellationDetails {
                                                    is_not_found: true,
                                                    is_timed_out: true,
                                                    ..Default::default()
                                                },
                                            ));
                                        }));
                                    if let Some(outstanding_info) =
                                        self.outstanding_tasks.lock().get_mut(&tt)
                                    {
                                        outstanding_info.local_timeouts_task = local_timeouts_task;
                                        outstanding_info.timeout_resetter = resetter;
                                    }
                                }
                            }

                            ActivityTask::start_from_poll_resp(task.resp)
                        }))
                    }
                };
                async move { res }
            })
            .take_until(async move {
                // Once we've been told to begin cancelling, wait the grace period and then start
                // cancelling anything outstanding.
                let (grace_killer, stop_grace) = futures_util::future::abortable(async {
                    if let Some(gp) = self.grace_period {
                        self.shutdown_initiated_token.cancelled().await;
                        tokio::time::sleep(gp).await;
                        should_issue_immediate_cancel_clone.store(true, Ordering::Release);
                        for task_token in outstanding_tasks_clone
                            .lock()
                            .keys()
                            .cloned()
                            .collect::<Vec<_>>()
                        {
                            let _ = self.cancels_tx.send(PendingActivityCancel::new(
                                task_token,
                                ActivityCancelReason::WorkerShutdown,
                                ActivityTask::primary_reason_to_cancellation_details(
                                    ActivityCancelReason::WorkerShutdown,
                                ),
                            ));
                        }
                    }
                });
                join!(
                    async {
                        self.start_tasks_stream_complete.cancelled().await;
                        while {
                            let outstanding_tasks = outstanding_tasks_clone.lock();
                            !outstanding_tasks.is_empty()
                        } {
                            self.complete_notify.notified().await
                        }
                        // If we were waiting for the grace period but everything already finished,
                        // we don't need to keep waiting.
                        stop_grace.abort();
                    },
                    grace_killer
                )
            })
    }
}

/// Provides facilities for the workflow side of things to interact with the activity manager.
/// Allows for the handling of activities returned by WFT completions.
pub(crate) struct ActivitiesFromWFTsHandle {
    sem: Arc<ClosableMeteredPermitDealer<ActivitySlotKind>>,
    tx: UnboundedSender<TrackedPermittedTqResp<PollActivityTaskQueueResponse>>,
}

impl ActivitiesFromWFTsHandle {
    /// Returns a handle that can be used to reserve an activity slot. EX: When requesting eager
    /// dispatch of an activity to this worker upon workflow task completion
    pub(crate) fn reserve_slot(&self) -> Option<TrackedOwnedMeteredSemPermit<ActivitySlotKind>> {
        // TODO: check if rate limit is not exceeded and count this reservation towards the rate limit
        self.sem.try_acquire_owned().ok()
    }

    /// Queue new activity tasks for dispatch received from non-polling sources (ex: eager returns
    /// from WFT completion)
    pub(crate) fn add_tasks(
        &self,
        tasks: impl IntoIterator<Item = TrackedPermittedTqResp<PollActivityTaskQueueResponse>>,
    ) {
        for t in tasks.into_iter() {
            // Technically we should be reporting `activity_task_received` here, but for simplicity
            // and time insensitivity, that metric is tracked in `about_to_issue_task`.
            self.tx.send(t).expect("Receive half cannot be dropped");
        }
    }
}

fn worker_shutdown_failure() -> Failure {
    Failure {
        message: "Worker is shutting down and this activity did not complete in time".to_string(),
        source: "".to_string(),
        stack_trace: "".to_string(),
        encoded_attributes: None,
        cause: None,
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: "WorkerShutdown".to_string(),
                non_retryable: false,
                ..Default::default()
            },
        )),
    }
}

/// The failure is deliberately retryable: catching the violation client-side exists precisely to
/// turn what the server would hard-fail into a recoverable activity task failure, so fixing and
/// redeploying the activity lets the next attempt succeed.
pub(super) fn make_payloads_too_large_failure(violation: &PayloadLimitViolation) -> Failure {
    Failure {
        message: violation.to_string(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: crate::worker::PAYLOADS_TOO_LARGE_FAILURE_TYPE.to_string(),
                non_retryable: false,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// Which of an activity's two local timers fired first. Returned from
/// [`ActivityLocalTimers::run`] and used downstream only for logging.
#[derive(Debug, Clone, Copy)]
enum ActivityLocalTimeoutKind {
    Heartbeat,
    StartToClose,
}

impl std::fmt::Display for ActivityLocalTimeoutKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Heartbeat => "heartbeat",
            Self::StartToClose => "start_to_close",
        })
    }
}

/// Races a (resettable) heartbeat timer against a (non-resettable)
/// `start_to_close` timer for one activity attempt. The heartbeat timer is
/// reset every time the held [`Notify`] is signalled (by the heartbeat
/// manager, when a heartbeat RPC ack arrives); the `start_to_close` timer
/// just counts down. Whichever fires first wins.
///
/// Construction returns [`None`] when both timeouts are unset, so the
/// caller knows not to spawn a local-timeouts task at all.
struct ActivityLocalTimers {
    heartbeat: Option<(Duration, Arc<Notify>)>,
    start_to_close: Option<Duration>,
}

impl ActivityLocalTimers {
    fn new(
        heartbeat: Option<prost_types::Duration>,
        start_to_close: Option<prost_types::Duration>,
        local_timeout_buffer: Duration,
    ) -> Option<Self> {
        // Filter out 0 / non-set timeouts, and add timeout buffer.
        let to_sleep = |d: Option<prost_types::Duration>| -> Option<Duration> {
            d.and_then(|d| Duration::try_from(d).ok())
                .filter(|d| !d.is_zero())
                .map(|d| d + local_timeout_buffer)
        };

        let heartbeat = to_sleep(heartbeat);
        let start_to_close = to_sleep(start_to_close);

        if heartbeat.is_none() && start_to_close.is_none() {
            return None;
        }
        Some(Self {
            heartbeat: heartbeat.map(|d| (d, Arc::new(Notify::new()))),
            start_to_close,
        })
    }

    /// A clone of the [`Notify`] that resets the heartbeat timer, for the
    /// heartbeat manager to signal. [`None`] when there's no heartbeat
    /// timer in play.
    fn heartbeat_resetter(&self) -> Option<Arc<Notify>> {
        self.heartbeat.as_ref().map(|t| t.1.clone())
    }

    /// Drive the two timers concurrently; resolves with whichever fires
    /// first.
    async fn run(self) -> ActivityLocalTimeoutKind {
        let heartbeat_timer = async {
            if let Some((sleep_time, rs)) = self.heartbeat {
                while tokio::time::timeout(sleep_time, rs.notified())
                    .await
                    .is_ok()
                {}
                ActivityLocalTimeoutKind::Heartbeat
            } else {
                std::future::pending().await
            }
        };
        let start_to_close_timer = async {
            if let Some(sleep_time) = self.start_to_close {
                tokio::time::sleep(sleep_time).await;
                ActivityLocalTimeoutKind::StartToClose
            } else {
                std::future::pending().await
            }
        };
        tokio::select! {
            t = heartbeat_timer => t,
            t = start_to_close_timer => t,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        pollers::{ActivityTaskOptions, LongPollBuffer},
        prost_dur,
        worker::{
            NamespaceCapabilities, PollerBehavior,
            client::{MockWorkerClient, mocks::mock_worker_client},
        },
    };
    use crossbeam_utils::atomic::AtomicCell;
    use temporalio_common::protos::coresdk::activity_result::ActivityExecutionResult;

    fn build_local_timeout_test_atm(
        mock_client: Arc<MockWorkerClient>,
        local_timeout_buffer: Duration,
    ) -> WorkerActivityTasks {
        let sem = fixed_size_permit_dealer(1);
        let shutdown_token = CancellationToken::new();
        let ap = LongPollBuffer::new_activity_task(
            mock_client.clone(),
            "tq".to_string(),
            PollerBehavior::SimpleMaximum(1),
            sem.clone(),
            shutdown_token,
            None::<fn(usize)>,
            ActivityTaskOptions {
                max_worker_acts_per_second: None,
                max_tps: None,
            },
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        );
        WorkerActivityTasks::new(
            sem,
            Box::new(ap),
            mock_client,
            MetricsContext::no_op(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
            local_timeout_buffer,
        )
    }

    // Confirms that a repeatedly-resetting heartbeat timer does not
    // prevent the start_to_close timer from firing.
    #[tokio::test]
    async fn activity_local_timers_start_to_close_wins_despite_resets() {
        let timers = ActivityLocalTimers::new(
            Some(prost_dur!(from_millis(100))),
            Some(prost_dur!(from_millis(300))),
            Duration::from_millis(0),
        )
        .expect("at least one timeout is set");
        let resetter = timers
            .heartbeat_resetter()
            .expect("heartbeat timer present");

        let reset_loop = async {
            loop {
                resetter.notify_one();
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        let kind = tokio::select! {
            kind = timers.run() => kind,
            _ = reset_loop => unreachable!("reset loop never exits on its own"),
        };
        assert!(
            matches!(kind, ActivityLocalTimeoutKind::StartToClose),
            "expected start_to_close to fire, got {kind:?}"
        );
    }

    #[tokio::test]
    async fn per_worker_ratelimit() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    ..Default::default()
                })
            });
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![2],
                    activity_id: "act2".to_string(),
                    ..Default::default()
                })
            });
        mock_client
            .expect_complete_activity_task()
            .times(2)
            .returning(|_, _| Ok(Default::default()));
        let mock_client = Arc::new(mock_client);
        let sem = fixed_size_permit_dealer(10);
        let shutdown_token = CancellationToken::new();
        let ap = LongPollBuffer::new_activity_task(
            mock_client.clone(),
            "tq".to_string(),
            // Lots of concurrent pollers, to ensure we don't poll to much when that's the case
            PollerBehavior::SimpleMaximum(5),
            sem.clone(),
            shutdown_token.clone(),
            None::<fn(usize)>,
            ActivityTaskOptions {
                max_worker_acts_per_second: Some(2.0),
                max_tps: None,
            },
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        );
        let atm = WorkerActivityTasks::new(
            sem.clone(),
            Box::new(ap),
            mock_client.clone(),
            MetricsContext::no_op(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
            Duration::from_secs(5),
        );
        let start = Instant::now();
        let t1 = atm.poll().await.unwrap();
        let t2 = atm.poll().await.unwrap();
        // At least half a second will have elapsed since we only allow 2 tasks per second.
        // With no ratelimit, even on a slow CI server with lots of load, this would typically take
        // low single digit ms or less.
        assert!(start.elapsed() > Duration::from_secs_f64(0.5));
        shutdown_token.cancel();
        // Need to complete the tasks so shutdown will resolve
        atm.complete(
            TaskToken(t1.task_token),
            ActivityExecutionResult::ok(vec![1].into()).status.unwrap(),
            mock_client.as_ref(),
        )
        .await;
        atm.complete(
            TaskToken(t2.task_token),
            ActivityExecutionResult::ok(vec![1].into()).status.unwrap(),
            mock_client.as_ref(),
        )
        .await;
        atm.initiate_shutdown();
        assert_matches!(atm.poll().await.unwrap_err(), PollError::ShutDown);
        atm.shutdown().await;
    }

    #[tokio::test]
    async fn local_timeouts() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_millis(100))),
                    // Verify zero durations do not apply
                    heartbeat_timeout: Some(prost_dur!(from_millis(0))),
                    ..Default::default()
                })
            });
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![2],
                    activity_id: "act2".to_string(),
                    heartbeat_timeout: Some(prost_dur!(from_millis(100))),
                    ..Default::default()
                })
            });
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![3],
                    activity_id: "act3".to_string(),
                    // Verify smaller of the timeouts is chosen
                    heartbeat_timeout: Some(prost_dur!(from_millis(100))),
                    start_to_close_timeout: Some(prost_dur!(from_secs(100))),
                    ..Default::default()
                })
            });
        let mock_client = Arc::new(mock_client);
        let atm = build_local_timeout_test_atm(mock_client.clone(), Duration::from_millis(100));

        for _ in 1..=3 {
            let start = Instant::now();
            let t = atm.poll().await.unwrap();
            // Just don't do anything when we get the task, wait for the timeout to come.
            let should_timeout = atm.poll().await.unwrap();
            assert!(should_timeout.is_timeout());
            assert!(start.elapsed() > Duration::from_millis(200));
            // Make sure it didn't take wayyy too long. Our long timeouts specified above are huge
            assert!(start.elapsed() < Duration::from_secs(5));
            atm.complete(
                TaskToken(t.task_token),
                ActivityExecutionResult::fail("unimportant".into())
                    .status
                    .unwrap(),
                mock_client.as_ref(),
            )
            .await;
        }

        atm.initiate_shutdown();
        assert_matches!(atm.poll().await.unwrap_err(), PollError::ShutDown);
        atm.shutdown().await;
    }

    #[tokio::test]
    async fn local_timeout_heartbeating() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(100))),
                    schedule_to_close_timeout: Some(prost_dur!(from_secs(100))),
                    heartbeat_timeout: Some(prost_dur!(from_millis(100))),
                    ..Default::default()
                })
            });
        mock_client // We can end up polling again - just return nothing.
            .expect_poll_activity_task()
            .returning(|_, _| Ok(Default::default()));
        mock_client
            .expect_record_activity_heartbeat()
            .times(2)
            .returning(|_, _| Ok(Default::default()));
        let mock_client = Arc::new(mock_client);
        let atm = build_local_timeout_test_atm(mock_client.clone(), Duration::from_millis(0));

        let t = atm.poll().await.unwrap();

        let heartbeater = async {
            for _ in 1..=2 {
                // Heartbeat twice within the timeout, but for a total time which would exceed it
                tokio::time::sleep(Duration::from_millis(60)).await;
                atm.record_heartbeat(ActivityHeartbeat {
                    task_token: t.task_token.clone(),
                    details: vec![],
                })
                .unwrap();
            }
        };
        let poller = async {
            let start = Instant::now();
            // We should now time out since we're failing to heartbeat again
            let should_timeout = atm.poll().await.unwrap();
            assert!(should_timeout.is_timeout());
            // Verify at least the two heartbeats elapsed before we got timed out
            assert!(start.elapsed() > Duration::from_millis(120));
        };

        join!(heartbeater, poller);

        atm.complete(
            TaskToken(t.task_token),
            ActivityExecutionResult::fail("unimportant".into())
                .status
                .unwrap(),
            mock_client.as_ref(),
        )
        .await;

        atm.initiate_shutdown();
        assert_matches!(atm.poll().await.unwrap_err(), PollError::ShutDown);
        atm.shutdown().await;
    }

    // Regression test for https://github.com/temporalio/sdk-rust/issues/1188.
    #[tokio::test]
    async fn start_to_close_fires_when_heartbeat_timeout_shorter() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_millis(300))),
                    heartbeat_timeout: Some(prost_dur!(from_millis(100))),
                    ..Default::default()
                })
            });
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(Default::default()));
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(Default::default()));
        let mock_client = Arc::new(mock_client);
        let atm = build_local_timeout_test_atm(mock_client.clone(), Duration::from_millis(0));

        let t = atm.poll().await.unwrap();
        let tt = t.task_token.clone();

        // Heartbeat every 50ms (faster than the 100ms heartbeat_timeout) so a
        // successful heartbeat-report always resets the heartbeat timer before
        // it can expire. start_to_close (300ms) must still fire.
        let heartbeater = async {
            loop {
                let _ = atm.record_heartbeat(ActivityHeartbeat {
                    task_token: tt.clone(),
                    details: vec![],
                });
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        // start_to_close_timeout=300ms. Allow generous slack (2s) before we
        // declare the bug present, to keep this stable on slow CI.
        let poller = async {
            tokio::time::timeout(Duration::from_secs(2), atm.poll())
                .await
                .expect("start_to_close_timeout was not enforced locally")
                .unwrap()
        };
        let activity_task = tokio::select! {
            v = poller => v,
            _ = heartbeater => unreachable!("heartbeat loop never exits on its own"),
        };
        assert!(activity_task.is_timeout());

        atm.complete(
            TaskToken(t.task_token),
            ActivityExecutionResult::fail("unimportant".into())
                .status
                .unwrap(),
            mock_client.as_ref(),
        )
        .await;

        atm.initiate_shutdown();
        assert_matches!(atm.poll().await.unwrap_err(), PollError::ShutDown);
        atm.shutdown().await;
    }
}
