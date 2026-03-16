use crate::{
    MetricsContext, TaskToken,
    abstractions::{MeteredPermitDealer, OwnedMeteredSemPermit, UsedMeteredSemPermit, dbg_panic},
    protosext::ValidScheduleLA,
    retry_logic::RetryPolicyExt,
    telemetry::metrics::{activity_type, should_record_failure_metric, workflow_type},
    worker::workflow::HeartbeatTimeoutMsg,
};
use futures_util::{
    Stream, StreamExt, future, future::AbortRegistration, stream, stream::BoxStream,
};
use parking_lot::{Mutex, MutexGuard};
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Debug, Formatter},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_api::worker::LocalActivitySlotKind;
use temporal_sdk_core_protos::{
    coresdk::{
        LocalActivitySlotInfo,
        activity_result::{Cancellation, Failure as ActFail, Success},
        activity_task::{ActivityCancelReason, ActivityTask, Start, activity_task},
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::TimeoutType,
        failure::v1::{Failure as APIFailure, TimeoutFailureInfo, failure},
    },
};
use tokio::{
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
    time::sleep,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

#[allow(clippy::large_enum_variant)] // Timeouts are relatively rare
#[derive(Debug)]
pub(crate) enum NextPendingLAAction {
    /// Send the activity task to lang
    Dispatch(ActivityTask),
    /// The worker must re-feed this completion back through the machines. Includes timeouts.
    Autocomplete(LACompleteAction),
}

#[derive(Debug)]
struct LocalInFlightActInfo {
    la_info: NewLocalAct,
    dispatch_time: Instant,
    attempt: u32,
    _permit: UsedMeteredSemPermit<LocalActivitySlotKind>,
}

#[derive(Debug, Clone)]
pub(crate) enum LocalActivityExecutionResult {
    Completed(Success),
    Failed(ActFail),
    TimedOut(ActFail),
    Cancelled(Cancellation),
}
impl LocalActivityExecutionResult {
    pub(crate) fn empty_cancel() -> Self {
        Self::Cancelled(Cancellation::from_details(None))
    }
    pub(crate) fn timeout(tt: TimeoutType) -> Self {
        Self::TimedOut(ActFail {
            failure: Some(APIFailure {
                message: "Activity timed out".to_string(),
                failure_info: Some(failure::FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo {
                        timeout_type: tt as i32,
                        last_heartbeat_details: None,
                    },
                )),
                ..Default::default()
            }),
        })
    }

    fn get_timeout_type(&self) -> Option<TimeoutType> {
        match self {
            Self::TimedOut(ActFail {
                failure:
                    Some(APIFailure {
                        failure_info:
                            Some(failure::FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                                timeout_type,
                                ..
                            })),
                        ..
                    }),
                ..
            }) => TimeoutType::try_from(*timeout_type).ok(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LocalActivityResolution {
    pub(crate) seq: u32,
    pub(crate) result: LocalActivityExecutionResult,
    pub(crate) runtime: Duration,
    pub(crate) attempt: u32,
    pub(crate) backoff: Option<prost_types::Duration>,
    pub(crate) original_schedule_time: Option<SystemTime>,
}

#[derive(Clone)]
pub(crate) struct NewLocalAct {
    pub(crate) schedule_cmd: ValidScheduleLA,
    pub(crate) workflow_type: String,
    pub(crate) workflow_exec_info: WorkflowExecution,
    pub(crate) schedule_time: SystemTime,
}

impl Debug for NewLocalAct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalActivity({}, {})",
            self.schedule_cmd.seq, self.schedule_cmd.activity_type
        )
    }
}

#[derive(Debug, derive_more::From)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum LocalActRequest {
    New(NewLocalAct),
    Cancel(ExecutingLAId),
    #[from(ignore)]
    CancelAllInRun(String),
    StartHeartbeatTimeout {
        send_on_elapse: HeartbeatTimeoutMsg,
        deadline: Instant,
        abort_reg: AbortRegistration,
    },
    /// Tell the LA manager that a workflow task was responded to (completed or failed) for a
    /// certain run id
    #[from(ignore)]
    IndicateWorkflowTaskCompleted(String),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct ExecutingLAId {
    pub(crate) run_id: String,
    pub(crate) seq_num: u32,
}

pub(crate) struct LocalActivityManager {
    /// Just so we can provide activity tasks the same namespace as the worker
    namespace: String,
    /// Sink for new activity execution requests
    act_req_tx: UnboundedSender<NewOrRetry>,
    /// Cancels need a different queue since they should be taken first, and don't take a permit
    cancels_req_tx: UnboundedSender<CancelOrTimeout>,
    /// For the emission of heartbeat timeouts, back into the workflow machines. This channel
    /// needs to come in from above us, because we cannot rely on callers getting the next
    /// activation as a way to deliver heartbeats.
    heartbeat_timeout_tx: UnboundedSender<HeartbeatTimeoutMsg>,
    /// Wakes every time a complete is processed
    complete_notify: Notify,
    /// Set once workflows have finished shutting down, and thus we know we will no longer receive
    /// any requests to spawn new LAs
    workflows_have_shut_down: CancellationToken,

    rcvs: tokio::sync::Mutex<RcvChans>,
    shutdown_complete_tok: CancellationToken,
    dat: Mutex<LAMData>,
    /// Note that these metrics do *not* include the `worker_type` label, as every metric
    /// emitted here is already specific to local activities via the metric name.
    metrics: MetricsContext,
}

struct LocalActivityInfo {
    task_token: TaskToken,
    /// Tasks for the current backoff until the next retry, if any.
    backing_off_task: Option<JoinHandle<()>>,
    /// Tasks / info about timeouts associated with this LA. May be empty for very brief periods
    /// while the LA id has been generated, but it has not yet been scheduled.
    timeout_bag: Option<TimeoutBag>,
    /// True once the first workflow task this LA started in has elapsed
    first_wft_has_ended: bool,
    /// Attempts at executing this LA during the current WFT
    attempts_in_wft: usize,
}

struct LAMData {
    /// Maps local activity identifiers to information about them
    la_info: HashMap<ExecutingLAId, LocalActivityInfo>,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: HashMap<TaskToken, LocalInFlightActInfo>,
    next_tt_num: u32,
}

impl LAMData {
    fn gen_next_token(&mut self) -> TaskToken {
        self.next_tt_num += 1;
        TaskToken::new_local_activity_token(self.next_tt_num.to_le_bytes())
    }
}

impl LocalActivityManager {
    pub(crate) fn new(
        namespace: String,
        permit_dealer: MeteredPermitDealer<LocalActivitySlotKind>,
        heartbeat_timeout_tx: UnboundedSender<HeartbeatTimeoutMsg>,
        metrics_context: MetricsContext,
    ) -> Self {
        let (act_req_tx, act_req_rx) = unbounded_channel();
        let (cancels_req_tx, cancels_req_rx) = unbounded_channel();
        let shutdown_complete_tok = CancellationToken::new();
        Self {
            namespace,
            rcvs: tokio::sync::Mutex::new(RcvChans::new(
                act_req_rx,
                permit_dealer,
                cancels_req_rx,
                shutdown_complete_tok.clone(),
            )),
            act_req_tx,
            cancels_req_tx,
            heartbeat_timeout_tx,
            complete_notify: Notify::new(),
            shutdown_complete_tok,
            dat: Mutex::new(LAMData {
                outstanding_activity_tasks: Default::default(),
                la_info: Default::default(),
                next_tt_num: 0,
            }),
            workflows_have_shut_down: Default::default(),
            metrics: metrics_context,
        }
    }

    #[cfg(test)]
    fn test(max_concurrent: usize) -> Self {
        use crate::worker::tuner::FixedSizeSlotSupplier;
        use std::sync::Arc;

        let ss = Arc::new(FixedSizeSlotSupplier::new(max_concurrent));
        let (hb_tx, _hb_rx) = unbounded_channel();
        Self::new(
            "fake_ns".to_string(),
            MeteredPermitDealer::new(
                ss,
                MetricsContext::no_op(),
                None,
                Arc::new(Default::default()),
                None,
            ),
            hb_tx,
            MetricsContext::no_op(),
        )
    }

    #[cfg(test)]
    pub(crate) fn num_outstanding(&self) -> usize {
        self.dat.lock().outstanding_activity_tasks.len()
    }

    #[cfg(test)]
    fn num_in_backoff(&self) -> usize {
        self.dat
            .lock()
            .la_info
            .values()
            .filter(|lai| lai.backing_off_task.is_some())
            .count()
    }

    pub(crate) fn enqueue(
        &self,
        reqs: impl IntoIterator<Item = LocalActRequest>,
    ) -> Vec<LocalActivityResolution> {
        if self.workflows_have_shut_down.is_cancelled() {
            dbg_panic!("Tried to enqueue local activity after workflows were shut down");
            return vec![];
        }
        let mut immediate_resolutions = vec![];
        for req in reqs {
            match req {
                LocalActRequest::New(act) => {
                    debug!(local_activity=?act, "Queuing local activity");
                    let id = ExecutingLAId {
                        run_id: act.workflow_exec_info.run_id.clone(),
                        seq_num: act.schedule_cmd.seq,
                    };
                    let mut dlock = self.dat.lock();
                    let tt = dlock.gen_next_token();
                    match dlock.la_info.entry(id) {
                        Entry::Occupied(o) => {
                            // Do not queue local activities which are in fact already executing.
                            // This can happen during evictions.
                            debug!(
                                "Tried to queue already-executing local activity {:?}",
                                o.key()
                            );
                            continue;
                        }
                        Entry::Vacant(ve) => {
                            // Insert the task token now, before we may or may not dispatch the
                            // activity, so we can enforce idempotency. Prevents two identical LAs
                            // ending up in the queue at once.
                            let lai = ve.insert(LocalActivityInfo {
                                task_token: tt,
                                backing_off_task: None,
                                timeout_bag: None,
                                first_wft_has_ended: false,
                                attempts_in_wft: 0,
                            });

                            // Set up timeouts for the new activity
                            match TimeoutBag::new(&act, self.cancels_req_tx.clone()) {
                                Ok(tb) => {
                                    lai.timeout_bag = Some(tb);

                                    self.act_req_tx.send(NewOrRetry::New(act)).expect(
                                        "Receive half of LA request channel cannot be dropped",
                                    );
                                }
                                Err(res) => immediate_resolutions.push(res),
                            }
                        }
                    }
                }
                LocalActRequest::StartHeartbeatTimeout {
                    send_on_elapse,
                    deadline,
                    abort_reg,
                } => {
                    let chan = self.heartbeat_timeout_tx.clone();
                    tokio::spawn(future::Abortable::new(
                        async move {
                            tokio::time::sleep_until(deadline.into()).await;
                            let _ = chan.send(send_on_elapse);
                        },
                        abort_reg,
                    ));
                }
                LocalActRequest::Cancel(id) => {
                    debug!(id=?id, "Cancelling local activity");
                    let mut dlock = self.dat.lock();
                    if let Some(lai) = dlock.la_info.get_mut(&id)
                        && let Some(immediate_res) = self.cancel_one_la(id.seq_num, lai)
                    {
                        immediate_resolutions.push(immediate_res);
                    }
                }
                LocalActRequest::CancelAllInRun(run_id) => {
                    debug!(run_id=%run_id, "Cancelling all local activities for run");
                    let mut dlock = self.dat.lock();
                    // Even if we've got 100k+ LAs this should only take a ms or two. Not worth
                    // adding another map to keep in sync.
                    let las_for_run = dlock
                        .la_info
                        .iter_mut()
                        .filter(|(id, _)| id.run_id == run_id);
                    for (laid, lainf) in las_for_run {
                        if let Some(immediate_res) = self.cancel_one_la(laid.seq_num, lainf) {
                            immediate_resolutions.push(immediate_res);
                        }
                    }
                }
                LocalActRequest::IndicateWorkflowTaskCompleted(run_id) => {
                    let mut dlock = self.dat.lock();
                    let las_for_run = dlock
                        .la_info
                        .iter_mut()
                        .filter(|(id, _)| id.run_id == run_id);
                    for (_, lainf) in las_for_run {
                        lainf.first_wft_has_ended = true;
                        lainf.attempts_in_wft = 0;
                    }
                }
            }
        }
        immediate_resolutions
    }

    /// Returns the next pending local-activity related action, or None if shutdown has initiated
    /// and there are no more remaining actions to take.
    pub(crate) async fn next_pending(&self) -> Option<NextPendingLAAction> {
        let (new_or_retry, permit) = match self.rcvs.lock().await.next().await? {
            NewOrCancel::Cancel(c) => {
                return match c {
                    CancelOrTimeout::Cancel(c) => {
                        if self
                            .dat
                            .lock()
                            .outstanding_activity_tasks
                            .contains_key(c.task_token.as_slice())
                        {
                            Some(NextPendingLAAction::Dispatch(c))
                        } else {
                            // Don't dispatch cancels for things we've already stopped tracking
                            None
                        }
                    }
                    CancelOrTimeout::Timeout { run_id, resolution } => {
                        let tt = self
                            .dat
                            .lock()
                            .la_info
                            .get(&ExecutingLAId {
                                run_id,
                                seq_num: resolution.seq,
                            })
                            .as_ref()
                            .map(|lai| lai.task_token.clone());
                        if let Some(task_token) = tt {
                            Some(NextPendingLAAction::Autocomplete(
                                self.complete(&task_token, resolution.result),
                            ))
                        } else {
                            // This timeout is for a no-longer-tracked activity, so, whatever
                            None
                        }
                    }
                };
            }
            NewOrCancel::New(n, perm) => (n, perm),
        };

        // It is important that there are no await points after receiving from the channel, as
        // it would mean dropping this future would cause us to drop the activity request.
        let (new_la, attempt) = match new_or_retry {
            NewOrRetry::New(n) => {
                let explicit_attempt_num_or_1 = n.schedule_cmd.attempt.max(1);
                (n, explicit_attempt_num_or_1)
            }
            NewOrRetry::Retry { in_flight, attempt } => (in_flight, attempt),
        };
        let la_info_for_in_flight_map = new_la.clone();
        let id = ExecutingLAId {
            run_id: new_la.workflow_exec_info.run_id.clone(),
            seq_num: new_la.schedule_cmd.seq,
        };
        let orig_sched_time = new_la.schedule_cmd.original_schedule_time;
        let sa = new_la.schedule_cmd;

        let mut dat = self.dat.lock();
        // If this request originated from a local backoff task, clear the entry for it. We
        // don't await the handle because we know it must already be done, and there's no
        // meaningful value.
        dat.la_info
            .get_mut(&id)
            .map(|lai| lai.backing_off_task.take());

        // If this task sat in the queue for too long, return a timeout for it instead
        if let Some(s2s) = sa.schedule_to_start_timeout.as_ref() {
            let sat_for = new_la.schedule_time.elapsed().unwrap_or_default();
            if sat_for > *s2s {
                return Some(NextPendingLAAction::Autocomplete(
                    LACompleteAction::Report {
                        run_id: new_la.workflow_exec_info.run_id,
                        resolution: LocalActivityResolution {
                            seq: sa.seq,
                            result: LocalActivityExecutionResult::timeout(
                                TimeoutType::ScheduleToStart,
                            ),
                            runtime: sat_for,
                            attempt,
                            backoff: None,
                            original_schedule_time: orig_sched_time,
                        },
                        task: None,
                    },
                ));
            }
        }

        let la_info = dat.la_info.get_mut(&id).expect("Activity must exist");
        let tt = la_info.task_token.clone();
        if let Some(to) = la_info.timeout_bag.as_mut() {
            to.mark_started();
        }
        dat.outstanding_activity_tasks.insert(
            tt.clone(),
            LocalInFlightActInfo {
                la_info: la_info_for_in_flight_map,
                dispatch_time: Instant::now(),
                attempt,
                _permit: permit.into_used(LocalActivitySlotInfo {
                    activity_type: new_la.workflow_type.clone(),
                }),
            },
        );

        let (schedule_to_close, start_to_close) = sa.close_timeouts.into_sched_and_start();
        self.metrics
            .with_new_attrs([
                activity_type(sa.activity_type.clone()),
                workflow_type(new_la.workflow_type.clone()),
            ])
            .la_executed();
        Some(NextPendingLAAction::Dispatch(ActivityTask {
            task_token: tt.0,
            variant: Some(activity_task::Variant::Start(Start {
                workflow_namespace: self.namespace.clone(),
                workflow_type: new_la.workflow_type,
                workflow_execution: Some(new_la.workflow_exec_info),
                activity_id: sa.activity_id,
                activity_type: sa.activity_type,
                header_fields: sa.headers,
                input: sa.arguments,
                heartbeat_details: vec![],
                scheduled_time: Some(new_la.schedule_time.into()),
                current_attempt_scheduled_time: Some(new_la.schedule_time.into()),
                started_time: Some(SystemTime::now().into()),
                attempt,
                schedule_to_close_timeout: schedule_to_close
                    .unwrap_or(Duration::ZERO)
                    .try_into()
                    .ok(),
                start_to_close_timeout: start_to_close
                    .or(schedule_to_close)
                    .and_then(|t| t.try_into().ok()),
                heartbeat_timeout: None,
                retry_policy: Some(sa.retry_policy),
                priority: Some(Default::default()),
                is_local: true,
            })),
        }))
    }

    /// Mark a local activity as having completed
    pub(crate) fn complete(
        &self,
        task_token: &TaskToken,
        status: LocalActivityExecutionResult,
    ) -> LACompleteAction {
        let mut dlock = self.dat.lock();
        if let Some(info) = dlock.outstanding_activity_tasks.remove(task_token) {
            if self.workflows_have_shut_down.is_cancelled() {
                // If workflows are already shut down, the results of all this don't matter.
                // Just say we're done if there's nothing outstanding any more.
                self.set_shutdown_complete_if_ready(&mut dlock);
            }

            let exec_id = ExecutingLAId {
                run_id: info.la_info.workflow_exec_info.run_id.clone(),
                seq_num: info.la_info.schedule_cmd.seq,
            };
            let maybe_old_lai = dlock.la_info.remove(&exec_id);
            if let Some(ref oldlai) = maybe_old_lai
                && let Some(ref bot) = oldlai.backing_off_task
            {
                dbg_panic!("Just-resolved LA should not have backoff task");
                bot.abort();
            }

            let la_metrics = self.metrics.with_new_attrs([
                activity_type(info.la_info.schedule_cmd.activity_type.clone()),
                workflow_type(info.la_info.workflow_type.clone()),
            ]);

            enum Outcome {
                FailurePath { backoff: Option<Duration> },
                JustReport,
            }
            macro_rules! calc_backoff {
                ($fail: ident) => {
                    info.la_info.schedule_cmd.retry_policy.should_retry(
                        info.attempt as usize,
                        $fail
                            .failure
                            .as_ref()
                            .and_then(|f| f.maybe_application_failure()),
                    )
                };
            }

            let mut is_timeout = false;
            let runtime = info.dispatch_time.elapsed();
            la_metrics.la_exec_latency(runtime);
            let outcome = match &status {
                LocalActivityExecutionResult::Failed(fail) => {
                    if should_record_failure_metric(&fail.failure) {
                        la_metrics.la_execution_failed()
                    }
                    Outcome::FailurePath {
                        backoff: calc_backoff!(fail),
                    }
                }
                LocalActivityExecutionResult::TimedOut(fail) => {
                    la_metrics.la_execution_failed();
                    is_timeout = true;
                    // Start to close timeouts are retryable, other timeout types aren't.
                    if matches!(status.get_timeout_type(), Some(TimeoutType::StartToClose)) {
                        Outcome::FailurePath {
                            backoff: calc_backoff!(fail),
                        }
                    } else {
                        Outcome::JustReport
                    }
                }
                LocalActivityExecutionResult::Completed(_) => {
                    if let Some(rt) = info
                        .la_info
                        .schedule_cmd
                        .original_schedule_time
                        .and_then(|t| t.elapsed().ok())
                    {
                        la_metrics.la_exec_succeeded_latency(rt);
                    }
                    Outcome::JustReport
                }
                LocalActivityExecutionResult::Cancelled { .. } => {
                    la_metrics.la_execution_cancelled();
                    Outcome::JustReport
                }
            };

            let mut resolution = LocalActivityResolution {
                seq: info.la_info.schedule_cmd.seq,
                result: status,
                runtime,
                attempt: info.attempt,
                backoff: None,
                original_schedule_time: info.la_info.schedule_cmd.original_schedule_time,
            };
            // We want to generate a cancel task if the reason for failure was a timeout.
            let task = if is_timeout {
                Some(ActivityTask::cancel_from_ids(
                    task_token.clone().0,
                    ActivityCancelReason::TimedOut,
                    ActivityTask::primary_reason_to_cancellation_details(
                        ActivityCancelReason::TimedOut,
                    ),
                ))
            } else {
                None
            };

            match outcome {
                Outcome::FailurePath { backoff } => {
                    if let Some(backoff_dur) = backoff {
                        let fail_or_timeout = if is_timeout { "timed out" } else { "failed" };
                        let will_use_timer =
                            backoff_dur > info.la_info.schedule_cmd.local_retry_threshold;
                        debug!(run_id = %info.la_info.workflow_exec_info.run_id,
                               seq_num = %info.la_info.schedule_cmd.seq,
                               attempt = %info.attempt,
                               will_use_timer,
                            "Local activity {}, will retry after backing off for {:?}",
                            fail_or_timeout,
                            backoff_dur
                        );
                        if will_use_timer {
                            // This la needs to write a failure marker, and then we will tell lang how
                            // long of a timer to schedule to back off for. We do this because there are
                            // no other situations where core generates "internal" commands so it is
                            // much simpler for lang to reply with the timer / next LA command than to
                            // do it internally. Plus, this backoff hack we'd like to eliminate
                            // eventually.
                            resolution.backoff =
                                Some(backoff_dur.try_into().expect("backoff fits into proto"));
                            return LACompleteAction::Report {
                                run_id: info.la_info.workflow_exec_info.run_id,
                                resolution,
                                task,
                            };
                        }
                        // Immediately create a new task token for the to-be-retried LA
                        let tt = dlock.gen_next_token();
                        // Send the retry request after waiting the backoff duration
                        let send_chan = self.act_req_tx.clone();
                        let jh = tokio::spawn(async move {
                            tokio::time::sleep(backoff_dur).await;

                            send_chan
                                .send(NewOrRetry::Retry {
                                    in_flight: info.la_info,
                                    attempt: info.attempt + 1,
                                })
                                .expect("Receive half of LA request channel cannot be dropped");
                        });
                        dlock.la_info.insert(
                            exec_id,
                            LocalActivityInfo {
                                task_token: tt,
                                backing_off_task: Some(jh),
                                first_wft_has_ended: maybe_old_lai
                                    .as_ref()
                                    .map(|old| old.first_wft_has_ended)
                                    .unwrap_or_default(),
                                attempts_in_wft: maybe_old_lai
                                    .as_ref()
                                    .map(|old| old.attempts_in_wft + 1)
                                    .unwrap_or(1),
                                timeout_bag: maybe_old_lai.and_then(|old| old.timeout_bag),
                            },
                        );
                        LACompleteAction::WillBeRetried(task)
                    } else {
                        LACompleteAction::Report {
                            run_id: info.la_info.workflow_exec_info.run_id,
                            resolution,
                            task,
                        }
                    }
                }
                Outcome::JustReport => {
                    self.complete_notify.notify_one();
                    LACompleteAction::Report {
                        run_id: info.la_info.workflow_exec_info.run_id,
                        resolution,
                        task,
                    }
                }
            }
        } else {
            if !matches!(
                status,
                LocalActivityExecutionResult::TimedOut(_)
                    | LocalActivityExecutionResult::Cancelled { .. }
            ) {
                warn!("Tried to complete untracked local activity");
            }
            LACompleteAction::Untracked
        }
    }

    pub(crate) fn workflows_have_shutdown(&self) {
        self.workflows_have_shut_down.cancel();
        self.set_shutdown_complete_if_ready(&mut self.dat.lock());
    }

    pub(crate) async fn wait_all_outstanding_tasks_finished(&self) {
        while !self.set_shutdown_complete_if_ready(&mut self.dat.lock()) {
            self.complete_notify.notified().await;
        }
        // This makes sure we drop any permits that might be held inside the stream
        self.rcvs.lock().await.inner = stream::empty().boxed();
    }

    /// Try to close the activity stream as soon as worker shutdown is initiated. This is required
    /// for activity-only workers where since workflows are not polled and the activity poller might
    /// get "stuck".
    pub(crate) fn shutdown_initiated(&self) {
        self.set_shutdown_complete_if_ready(&mut self.dat.lock());
    }

    pub(crate) fn get_nonfirst_attempt_count(&self, for_run_id: &str) -> usize {
        let dlock = self.dat.lock();
        dlock
            .la_info
            .iter()
            .filter(|(id, info)| id.run_id == for_run_id && info.first_wft_has_ended)
            .map(|(_, info)| info.attempts_in_wft)
            .sum()
    }

    fn set_shutdown_complete_if_ready(&self, dlock: &mut MutexGuard<LAMData>) -> bool {
        let nothing_outstanding = dlock.outstanding_activity_tasks.is_empty();
        if nothing_outstanding && self.workflows_have_shut_down.is_cancelled() {
            self.shutdown_complete_tok.cancel();
        }
        nothing_outstanding
    }

    fn cancel_one_la(
        &self,
        seq: u32,
        lai: &mut LocalActivityInfo,
    ) -> Option<LocalActivityResolution> {
        // First check if this ID is currently backing off, if so abort the backoff
        // task
        if let Some(t) = lai.backing_off_task.take() {
            t.abort();
            return Some(LocalActivityResolution {
                seq,
                result: LocalActivityExecutionResult::Cancelled(Cancellation::from_details(None)),
                runtime: Duration::from_secs(0),
                attempt: 0,
                backoff: None,
                original_schedule_time: None,
            });
        }

        self.cancels_req_tx
            .send(CancelOrTimeout::Cancel(ActivityTask::cancel_from_ids(
                lai.task_token.0.clone(),
                ActivityCancelReason::Cancelled,
                ActivityTask::primary_reason_to_cancellation_details(
                    ActivityCancelReason::Cancelled,
                ),
            )))
            .expect("Receive half of LA cancel channel cannot be dropped");
        None
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Most will be reported
pub(crate) enum LACompleteAction {
    /// Caller should report the status to the workflow machines - which may resolve the activity
    /// or might result in scheduling a backoff timer.
    Report {
        run_id: String,
        resolution: LocalActivityResolution,
        /// May be set if a task also needs to be dispatched to lang. EX: Cancelling a timed-out
        /// activity.
        task: Option<ActivityTask>,
    },
    /// The activity will be re-enqueued for another attempt (and so status should not be reported
    /// to the workflow)
    WillBeRetried(Option<ActivityTask>),
    /// The activity was unknown
    Untracked,
}

#[derive(Debug)]
enum NewOrRetry {
    New(NewLocalAct),
    Retry {
        in_flight: NewLocalAct,
        attempt: u32,
    },
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
enum CancelOrTimeout {
    Cancel(ActivityTask),
    Timeout {
        run_id: String,
        resolution: LocalActivityResolution,
    },
}

#[allow(clippy::large_enum_variant)]
enum NewOrCancel {
    New(NewOrRetry, OwnedMeteredSemPermit<LocalActivitySlotKind>),
    Cancel(CancelOrTimeout),
}

#[pin_project::pin_project]
struct RcvChans {
    #[pin]
    inner: BoxStream<'static, NewOrCancel>,
}

impl RcvChans {
    fn new(
        new_reqs: UnboundedReceiver<NewOrRetry>,
        new_sem: MeteredPermitDealer<LocalActivitySlotKind>,
        cancels: UnboundedReceiver<CancelOrTimeout>,
        shutdown_completed: CancellationToken,
    ) -> Self {
        let cancel_stream = UnboundedReceiverStream::new(cancels).map(NewOrCancel::Cancel);
        let new_stream = UnboundedReceiverStream::new(new_reqs)
            // Get a permit for each new activity request
            .zip(stream::unfold(new_sem, |new_sem| async move {
                let permit = new_sem.acquire_owned().await;
                Some((permit, new_sem))
            }))
            .map(|(req, permit)| NewOrCancel::New(req, permit));
        Self {
            inner: tokio_stream::StreamExt::merge(cancel_stream, new_stream)
                .take_until(async move { shutdown_completed.cancelled().await })
                .boxed(),
        }
    }
}
impl Stream for RcvChans {
    type Item = NewOrCancel;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

struct TimeoutBag {
    sched_to_close_handle: JoinHandle<()>,
    start_to_close_dur_and_dat: Option<(Duration, CancelOrTimeout)>,
    start_to_close_handle: Option<JoinHandle<()>>,
    cancel_chan: UnboundedSender<CancelOrTimeout>,
}

impl TimeoutBag {
    /// Create new timeout tasks for the provided local activity. This must be called as soon
    /// as request to schedule it arrives.
    ///
    /// Returns error in the event the activity is *already* timed out
    #[allow(clippy::result_large_err)]
    fn new(
        new_la: &NewLocalAct,
        cancel_chan: UnboundedSender<CancelOrTimeout>,
    ) -> Result<TimeoutBag, LocalActivityResolution> {
        let (schedule_to_close, start_to_close) =
            new_la.schedule_cmd.close_timeouts.into_sched_and_start();

        let sched_time = new_la
            .schedule_cmd
            .original_schedule_time
            .unwrap_or(new_la.schedule_time);
        let resolution = LocalActivityResolution {
            seq: new_la.schedule_cmd.seq,
            result: LocalActivityExecutionResult::timeout(TimeoutType::ScheduleToClose),
            runtime: Default::default(),
            attempt: new_la.schedule_cmd.attempt,
            backoff: None,
            original_schedule_time: new_la.schedule_cmd.original_schedule_time,
        };
        // Remove any time already elapsed since the scheduling time
        let schedule_to_close = schedule_to_close
            .map(|s2c| s2c.saturating_sub(sched_time.elapsed().unwrap_or_default()));
        if let Some(ref s2c) = schedule_to_close
            && s2c.is_zero()
        {
            return Err(resolution);
        }
        let timeout_dat = CancelOrTimeout::Timeout {
            run_id: new_la.workflow_exec_info.run_id.clone(),
            resolution,
        };
        let start_to_close_dur_and_dat = start_to_close.map(|d| (d, timeout_dat.clone()));
        let fut_dat = schedule_to_close.map(|s2c| (s2c, timeout_dat));

        let cancel_chan_clone = cancel_chan.clone();
        let scheduling = tokio::spawn(async move {
            if let Some((timeout, dat)) = fut_dat {
                sleep(timeout).await;
                cancel_chan_clone
                    .send(dat)
                    .expect("receive half not dropped");
            }
        });
        Ok(TimeoutBag {
            sched_to_close_handle: scheduling,
            start_to_close_dur_and_dat,
            start_to_close_handle: None,
            cancel_chan,
        })
    }

    /// Must be called once the associated local activity has been started / dispatched to lang.
    fn mark_started(&mut self) {
        if let Some((start_to_close, mut dat)) = self.start_to_close_dur_and_dat.as_ref().cloned() {
            let started_t = Instant::now();
            let cchan = self.cancel_chan.clone();
            self.start_to_close_handle = Some(tokio::spawn(async move {
                sleep(start_to_close).await;
                if let CancelOrTimeout::Timeout { resolution, .. } = &mut dat {
                    resolution.result =
                        LocalActivityExecutionResult::timeout(TimeoutType::StartToClose);
                    resolution.runtime = started_t.elapsed();
                }

                cchan.send(dat).expect("receive half not dropped");
            }));
        }
    }
}

impl Drop for TimeoutBag {
    fn drop(&mut self) {
        self.sched_to_close_handle.abort();
        if let Some(x) = self.start_to_close_handle.as_ref() {
            x.abort()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prost_dur, protosext::LACloseTimeouts};
    use futures_util::FutureExt;
    use temporal_sdk_core_protos::temporal::api::{
        common::v1::RetryPolicy,
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    };
    use tokio::task::yield_now;

    impl NextPendingLAAction {
        fn unwrap(self) -> ActivityTask {
            match self {
                NextPendingLAAction::Dispatch(t) => t,
                _ => {
                    panic!("Non-dispatched action returned")
                }
            }
        }

        fn is_timeout(&self, with_task: bool) -> bool {
            matches!(
                self,
                Self::Autocomplete(LACompleteAction::Report{task, resolution, ..})
                if matches!(resolution.result, LocalActivityExecutionResult::TimedOut(_)) &&
                   task.is_some() == with_task
            )
        }
    }

    #[tokio::test]
    async fn max_concurrent_respected() {
        let lam = LocalActivityManager::test(1);
        lam.enqueue((1..=50).map(|i| {
            NewLocalAct {
                schedule_cmd: ValidScheduleLA {
                    seq: i,
                    activity_id: i.to_string(),
                    ..Default::default()
                },
                workflow_type: "".to_string(),
                workflow_exec_info: Default::default(),
                schedule_time: SystemTime::now(),
            }
            .into()
        }));
        for i in 1..=50 {
            let next = lam.next_pending().await.unwrap().unwrap();
            assert_matches!(
                next.variant.unwrap(),
                activity_task::Variant::Start(Start {activity_id, ..})
                    if activity_id == i.to_string()
            );
            let next_tt = TaskToken(next.task_token);
            let complete_branch = async {
                lam.complete(
                    &next_tt,
                    LocalActivityExecutionResult::Completed(Default::default()),
                )
            };
            tokio::select! {
                // Next call will not resolve until we complete the first
                _ = lam.next_pending() => {
                    panic!("Branch must not be selected")
                }
                _ = complete_branch => {}
            }
        }
    }

    #[tokio::test]
    async fn no_work_doesnt_deadlock_with_complete() {
        let lam = LocalActivityManager::test(5);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        let tt = TaskToken(next.task_token);
        tokio::select! {
            biased;

            _ = lam.next_pending() => {
                panic!("Complete branch must win")
            }
            _ = async {
                // Spin until the receive lock has been grabbed by the call to pending, to ensure
                // it's advanced enough
                while lam.rcvs.try_lock().is_ok() { yield_now().await; }
                lam.complete(&tt, LocalActivityExecutionResult::Completed(Default::default()));
            } => (),
        };
    }

    #[tokio::test]
    async fn can_cancel_in_flight() {
        let lam = LocalActivityManager::test(5);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        }
        .into()]);
        lam.next_pending().await.unwrap().unwrap();

        lam.enqueue([LocalActRequest::Cancel(ExecutingLAId {
            run_id: "run_id".to_string(),
            seq_num: 1,
        })]);
        let next = lam.next_pending().await.unwrap().unwrap();
        assert_matches!(next.variant.unwrap(), activity_task::Variant::Cancel(_));
    }

    #[tokio::test]
    async fn respects_timer_backoff_threshold() {
        let lam = LocalActivityManager::test(1);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_secs(1))),
                    backoff_coefficient: 10.0,
                    maximum_interval: Some(prost_dur!(from_secs(10))),
                    maximum_attempts: 10,
                    non_retryable_error_types: vec![],
                },
                local_retry_threshold: Duration::from_secs(5),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        let tt = TaskToken(next.task_token);
        let res = lam.complete(
            &tt,
            LocalActivityExecutionResult::Failed(Default::default()),
        );
        assert_matches!(res, LACompleteAction::Report{resolution, ..}
            if resolution.backoff.as_ref().unwrap().seconds == 10 && resolution.attempt == 5
        )
    }

    #[tokio::test]
    async fn respects_non_retryable_error_types() {
        let lam = LocalActivityManager::test(1);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: "1".to_string(),
                attempt: 1,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_secs(1))),
                    backoff_coefficient: 10.0,
                    maximum_interval: Some(prost_dur!(from_secs(10))),
                    maximum_attempts: 10,
                    non_retryable_error_types: vec!["TestError".to_string()],
                },
                local_retry_threshold: Duration::from_secs(5),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        let tt = TaskToken(next.task_token);
        let res = lam.complete(
            &tt,
            LocalActivityExecutionResult::Failed(ActFail {
                failure: Some(Failure {
                    failure_info: Some(FailureInfo::ApplicationFailureInfo(
                        ApplicationFailureInfo {
                            r#type: "TestError".to_string(),
                            non_retryable: false,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
            }),
        );
        assert_matches!(res, LACompleteAction::Report { .. });
    }

    #[tokio::test]
    async fn can_cancel_during_local_backoff() {
        let lam = LocalActivityManager::test(1);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_secs(10))),
                    backoff_coefficient: 1.0,
                    maximum_interval: Some(prost_dur!(from_secs(10))),
                    maximum_attempts: 10,
                    non_retryable_error_types: vec![],
                },
                local_retry_threshold: Duration::from_secs(500),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        let tt = TaskToken(next.task_token);
        lam.complete(
            &tt,
            LocalActivityExecutionResult::Failed(Default::default()),
        );
        // Cancel the activity, which is performing local backoff
        let immediate_res = lam.enqueue([LocalActRequest::Cancel(ExecutingLAId {
            run_id: "run_id".to_string(),
            seq_num: 1,
        })]);
        // It should not be present in the backoff tasks
        assert_eq!(lam.num_in_backoff(), 0);
        assert_eq!(lam.num_outstanding(), 0);
        // It should return a resolution to cancel
        assert_eq!(immediate_res.len(), 1);
        assert_matches!(
            immediate_res[0].result,
            LocalActivityExecutionResult::Cancelled { .. }
        );
    }

    #[tokio::test]
    async fn local_backoff_clears_handle_map_when_started() {
        let lam = LocalActivityManager::test(1);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    ..Default::default()
                },
                local_retry_threshold: Duration::from_secs(500),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        let tt = TaskToken(next.task_token);
        lam.complete(
            &tt,
            LocalActivityExecutionResult::Failed(Default::default()),
        );
        lam.next_pending().await.unwrap().unwrap();
        assert_eq!(lam.num_in_backoff(), 0);
        assert_eq!(lam.num_outstanding(), 1);
    }

    #[tokio::test]
    async fn sched_to_start_timeout() {
        let lam = LocalActivityManager::test(1);
        let timeout = Duration::from_millis(100);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    ..Default::default()
                },
                local_retry_threshold: Duration::from_secs(500),
                schedule_to_start_timeout: Some(timeout),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        }
        .into()]);

        // Wait more than the timeout before grabbing the task
        sleep(timeout + Duration::from_millis(10)).await;

        assert!(lam.next_pending().await.unwrap().is_timeout(false));
        assert_eq!(lam.num_in_backoff(), 0);
        assert_eq!(lam.num_outstanding(), 0);
    }

    #[rstest::rstest]
    #[case::schedule(true)]
    #[case::start(false)]
    #[tokio::test]
    async fn local_x_to_close_timeout(#[case] is_schedule: bool) {
        let lam = LocalActivityManager::test(1);
        let timeout = Duration::from_millis(100);
        let close_timeouts = if is_schedule {
            LACloseTimeouts::ScheduleOnly(timeout)
        } else {
            LACloseTimeouts::StartOnly(timeout)
        };
        lam.enqueue([NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    maximum_attempts: 1,
                    ..Default::default()
                },
                local_retry_threshold: Duration::from_secs(500),
                close_timeouts,
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        }
        .into()]);

        let next = lam.next_pending().await.unwrap().unwrap();
        assert_eq!(lam.num_in_backoff(), 0);
        assert_eq!(lam.num_outstanding(), 1);

        if let Some(activity_task::Variant::Start(start)) = next.variant {
            // Validate that timeouts reported to lang matches what server would have provided
            // if this had been a normal activity with the same timeout configuration.
            if is_schedule {
                assert_eq!(start.schedule_to_close_timeout, timeout.try_into().ok());
                assert_eq!(start.start_to_close_timeout, timeout.try_into().ok());
            } else {
                assert_eq!(
                    start.schedule_to_close_timeout,
                    Duration::ZERO.try_into().ok()
                );
                assert_eq!(start.start_to_close_timeout, timeout.try_into().ok());
            }
        };

        sleep(timeout + Duration::from_millis(10)).await;
        assert!(lam.next_pending().await.unwrap().is_timeout(true));
        assert_eq!(lam.num_outstanding(), 0);
    }

    #[tokio::test]
    async fn idempotency_enforced() {
        let lam = LocalActivityManager::test(10);
        let new_la = NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: "run_id".to_string(),
            },
            schedule_time: SystemTime::now(),
        };
        // Verify only one will get queued
        lam.enqueue([new_la.clone().into(), new_la.clone().into()]);
        lam.next_pending().await.unwrap().unwrap();
        assert_eq!(lam.num_outstanding(), 1);
        // There should be nothing else in the queue
        assert!(lam.rcvs.lock().await.next().now_or_never().is_none());

        // Verify that if we now enqueue the same act again, after the task is outstanding, we still
        // don't add it.
        lam.enqueue([new_la.into()]);
        assert_eq!(lam.num_outstanding(), 1);
        assert!(lam.rcvs.lock().await.next().now_or_never().is_none());
    }

    #[tokio::test]
    async fn nonfirst_la_attempt_count_is_accurate() {
        let run_id = "run_id";
        let lam = LocalActivityManager::test(10);
        let new_la = NewLocalAct {
            schedule_cmd: ValidScheduleLA {
                seq: 1,
                activity_id: 1.to_string(),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(1))),
                    backoff_coefficient: 1.0,
                    ..Default::default()
                },
                local_retry_threshold: Duration::from_secs(500),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: WorkflowExecution {
                workflow_id: "".to_string(),
                run_id: run_id.to_string(),
            },
            schedule_time: SystemTime::now(),
        };
        lam.enqueue([new_la.clone().into()]);
        let spinfail = || async {
            for _ in 1..=10 {
                let next = lam.next_pending().await.unwrap().unwrap();
                let tt = TaskToken(next.task_token);
                lam.complete(
                    &tt,
                    LocalActivityExecutionResult::Failed(Default::default()),
                );
            }
        };

        // Fail a bunch of times
        spinfail().await;
        // Nonfirst attempt count should still be zero
        let count = lam.get_nonfirst_attempt_count(run_id);
        assert_eq!(count, 0);

        for _ in 1..=2 {
            // This should work over multiple WFTs
            // say the first wft was completed
            lam.enqueue([LocalActRequest::IndicateWorkflowTaskCompleted(
                run_id.to_string(),
            )]);
            // Do some more attempts
            spinfail().await;
            let count = lam.get_nonfirst_attempt_count(run_id);
            assert_eq!(count, 10);
        }
    }
}
