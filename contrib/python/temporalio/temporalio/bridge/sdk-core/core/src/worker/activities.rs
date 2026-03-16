mod activity_heartbeat_manager;
mod local_activities;

pub(crate) use local_activities::{
    ExecutingLAId, LACompleteAction, LocalActRequest, LocalActivityExecutionResult,
    LocalActivityManager, LocalActivityResolution, NewLocalAct, NextPendingLAAction,
};

use crate::{
    PollError, TaskToken,
    abstractions::{
        ClosableMeteredPermitDealer, MeteredPermitDealer, TrackedOwnedMeteredSemPermit,
        UsedMeteredSemPermit,
    },
    pollers::{BoxedActPoller, PermittedTqResp, TrackedPermittedTqResp, new_activity_task_poller},
    telemetry::metrics::{
        MetricsContext, activity_type, eager, should_record_failure_metric, workflow_type,
    },
    worker::{
        activities::activity_heartbeat_manager::ActivityHeartbeatError, client::WorkerClient,
    },
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use futures_util::{
    Stream, StreamExt, stream,
    stream::{BoxStream, PollNext},
};
use std::{
    convert::TryInto,
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_api::worker::ActivitySlotKind;
use temporal_sdk_core_protos::{
    coresdk::{
        ActivityHeartbeat, ActivitySlotInfo,
        activity_result::{self as ar, activity_execution_result as aer},
        activity_task::{ActivityCancelReason, ActivityCancellationDetails, ActivityTask},
    },
    temporal::api::{
        failure::v1::{ApplicationFailureInfo, CanceledFailureInfo, Failure, failure::FailureInfo},
        workflowservice::v1::PollActivityTaskQueueResponse,
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

type OutstandingActMap = Arc<DashMap<TaskToken, RemoteInFlightActInfo>>;

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
        let outstanding_activity_tasks = Arc::new(DashMap::new());
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
        if let Some((_, act_info)) = self.outstanding_activity_tasks.remove(&task_token) {
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
            self.heartbeat_manager.evict(task_token.clone()).await;

            // No need to report activities which we already know the server doesn't care about
            if !known_not_found {
                let _flushing_guard = self.completers_lock.read().await;
                let maybe_net_err = match status {
                    aer::Status::WillCompleteAsync(_) => None,
                    aer::Status::Completed(ar::Success { result }) => {
                        if let Some(sched_time) = act_info
                            .base
                            .scheduled_time
                            .and_then(|st| st.elapsed().ok())
                        {
                            act_metrics.act_execution_succeeded(sched_time);
                        }
                        client
                            .complete_activity_task(task_token.clone(), result.map(Into::into))
                            .await
                            .err()
                    }
                    aer::Status::Failed(ar::Failure { failure }) => {
                        if should_record_failure_metric(&failure) {
                            act_metrics.act_execution_failed();
                        }
                        client
                            .fail_activity_task(task_token.clone(), failure)
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
                                )
                                .await
                                .err()
                        } else {
                            let details = if let Some(Failure {
                                failure_info:
                                    Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                                        details,
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
                            client
                                .cancel_activity_task(task_token.clone(), details)
                                .await
                                .err()
                        }
                    }
                };

                self.complete_notify.notify_waiters();

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
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(
        &self,
        details: ActivityHeartbeat,
    ) -> Result<(), ActivityHeartbeatError> {
        // TODO: Propagate these back as cancels. Silent fails is too nonobvious
        let at_info = self
            .outstanding_activity_tasks
            .get(&TaskToken(details.task_token.clone()))
            .ok_or(ActivityHeartbeatError::UnknownActivity)?;
        let heartbeat_timeout: Duration = at_info
            .heartbeat_timeout
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
            .record(details, throttle_interval, at_info.timeout_resetter.clone())
    }

    /// Returns a handle that the workflows management side can use to interact with this manager
    pub(crate) fn get_handle_for_workflows(&self) -> ActivitiesFromWFTsHandle {
        ActivitiesFromWFTsHandle {
            sem: self.eager_activities_semaphore.clone(),
            tx: self.eager_activities_tx.clone(),
        }
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
                        if let Some(mut details) =
                            self.outstanding_tasks.get_mut(&next_pc.task_token)
                        {
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
                            let outstanding_entry = self.outstanding_tasks.entry(tt.clone());
                            let mut outstanding_info =
                                outstanding_entry.insert(RemoteInFlightActInfo::new(
                                    &task.resp,
                                    task.permit.into_used(ActivitySlotInfo {
                                        activity_type: activity_type_name.to_string(),
                                    }),
                                ));
                            // If we have already waited the grace period and issued cancels,
                            // this will have been set true, indicating anything that happened
                            // to be buffered/in-flight/etc should get an immediate cancel. This
                            // is to allow the user to potentially decide to ignore cancels and
                            // do work on polls that got received during shutdown.
                            if should_issue_immediate_cancel.load(Ordering::Acquire) {
                                let _ = cancels_tx.send(PendingActivityCancel::new(
                                    tt,
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
                                let local_timeout_buffer = self.local_timeout_buffer;
                                static HEARTBEAT_TYPE: &str = "heartbeat";
                                let timeout_at = [
                                    (HEARTBEAT_TYPE, task.resp.heartbeat_timeout),
                                    ("start_to_close", task.resp.start_to_close_timeout),
                                ]
                                .into_iter()
                                .filter_map(|(k, d)| {
                                    d.and_then(|d| Duration::try_from(d).ok().map(|d| (k, d)))
                                })
                                .filter(|(_, d)| !d.is_zero())
                                .min_by(|(_, d1), (_, d2)| d1.cmp(d2));
                                if let Some((timeout_type, timeout_at)) = timeout_at {
                                    let sleep_time = timeout_at + local_timeout_buffer;
                                    let cancel_tx = cancels_tx.clone();
                                    let resetter = if timeout_type == HEARTBEAT_TYPE {
                                        Some(Arc::new(Notify::new()))
                                    } else {
                                        None
                                    };
                                    let resetter_clone = resetter.clone();
                                    outstanding_info.local_timeouts_task =
                                        Some(tokio::task::spawn(async move {
                                            if let Some(rs) = resetter_clone {
                                                loop {
                                                    tokio::select! {
                                                        _ = rs.notified() => continue,
                                                        _ = tokio::time::sleep(sleep_time) => break,
                                                    }
                                                }
                                            } else {
                                                tokio::time::sleep(sleep_time).await;
                                            }
                                            debug!(
                                                task_token=%tt,
                                                "Timing out activity due to elapsed local \
                                                 {timeout_type} timer"
                                            );
                                            let _ = cancel_tx.send(PendingActivityCancel::new(
                                                tt,
                                                ActivityCancelReason::TimedOut,
                                                ActivityCancellationDetails {
                                                    is_not_found: true,
                                                    is_timed_out: true,
                                                    ..Default::default()
                                                },
                                            ));
                                        }));
                                    outstanding_info.timeout_resetter = resetter;
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
                        for mapref in outstanding_tasks_clone.iter() {
                            let _ = self.cancels_tx.send(PendingActivityCancel::new(
                                mapref.key().clone(),
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
                        while !outstanding_tasks_clone.is_empty() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        pollers::{ActivityTaskOptions, LongPollBuffer},
        prost_dur,
        worker::client::mocks::mock_worker_client,
    };
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::coresdk::activity_result::ActivityExecutionResult;

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
        let sem = fixed_size_permit_dealer(1);
        let shutdown_token = CancellationToken::new();
        let ap = LongPollBuffer::new_activity_task(
            mock_client.clone(),
            "tq".to_string(),
            PollerBehavior::SimpleMaximum(1),
            sem.clone(),
            shutdown_token.clone(),
            None::<fn(usize)>,
            ActivityTaskOptions {
                max_worker_acts_per_second: None,
                max_tps: None,
            },
        );
        let atm = WorkerActivityTasks::new(
            sem.clone(),
            Box::new(ap),
            mock_client.clone(),
            MetricsContext::no_op(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
            Duration::from_millis(100), // Short buffer for unit test
        );

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
        let sem = fixed_size_permit_dealer(1);
        let shutdown_token = CancellationToken::new();
        let ap = LongPollBuffer::new_activity_task(
            mock_client.clone(),
            "tq".to_string(),
            PollerBehavior::SimpleMaximum(1),
            sem.clone(),
            shutdown_token.clone(),
            None::<fn(usize)>,
            ActivityTaskOptions {
                max_worker_acts_per_second: None,
                max_tps: None,
            },
        );
        let atm = WorkerActivityTasks::new(
            sem.clone(),
            Box::new(ap),
            mock_client.clone(),
            MetricsContext::no_op(),
            Duration::from_secs(1),
            Duration::from_secs(1),
            None,
            Duration::from_millis(0), // No buffer in this test
        );

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
}
