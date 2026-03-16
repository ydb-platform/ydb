use crate::{
    TaskToken,
    abstractions::take_cell::TakeCell,
    worker::{activities::PendingActivityCancel, client::WorkerClient},
};
use futures_util::StreamExt;
use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_protos::{
    coresdk::{
        ActivityHeartbeat, IntoPayloadsExt,
        activity_task::{ActivityCancelReason, ActivityCancellationDetails, ActivityTask},
    },
    temporal::api::{
        common::v1::Payload, workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    },
};
use tokio::{
    sync::{
        Notify,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Used to supply new heartbeat events to the activity heartbeat manager, or to send a shutdown
/// request.
pub(crate) struct ActivityHeartbeatManager {
    shutdown_token: CancellationToken,
    /// Used during `shutdown` to await until all inflight requests are sent.
    join_handle: TakeCell<JoinHandle<()>>,
    heartbeat_tx: UnboundedSender<HeartbeatAction>,
}

#[derive(Debug)]
enum HeartbeatAction {
    SendHeartbeat(ValidActivityHeartbeat),
    Evict {
        token: TaskToken,
        on_complete: Arc<Notify>,
    },
    CompleteReport(TaskToken),
    CompleteThrottle(TaskToken),
}

#[derive(Debug)]
struct ValidActivityHeartbeat {
    task_token: TaskToken,
    details: Vec<Payload>,
    throttle_interval: Duration,
    timeout_resetter: Option<Arc<Notify>>,
}

#[derive(Debug)]
enum HeartbeatExecutorAction {
    /// Heartbeats are throttled for this task token, sleep until duration or wait to be cancelled
    Sleep(TaskToken, Duration, CancellationToken),
    /// Report heartbeat to the server
    Report {
        task_token: TaskToken,
        details: Vec<Payload>,
    },
}

/// Errors thrown when heartbeating
#[derive(thiserror::Error, Debug)]
pub(crate) enum ActivityHeartbeatError {
    /// Heartbeat referenced an activity that we don't think exists. It may have completed already.
    #[error(
        "Heartbeat has been sent for activity that either completed or never started on this worker."
    )]
    UnknownActivity,
    /// There was a set heartbeat timeout, but it was not parseable. A valid timeout is requried
    /// to heartbeat.
    #[error("Unable to parse activity heartbeat timeout.")]
    InvalidHeartbeatTimeout,
}

/// Manages activity heartbeating for a worker. Allows sending new heartbeats or requesting and
/// awaiting for the shutdown. When shutdown is requested, signal gets sent to all processors, which
/// allows them to complete gracefully.
impl ActivityHeartbeatManager {
    /// Creates a new instance of an activity heartbeat manager and returns a handle to the user,
    /// which allows to send new heartbeats and initiate the shutdown.
    /// Returns the manager and a channel that buffers cancellation notifications to be sent to Lang.
    pub(super) fn new(
        client: Arc<dyn WorkerClient>,
        cancels_tx: UnboundedSender<PendingActivityCancel>,
    ) -> Self {
        let (heartbeat_stream_state, heartbeat_tx_source, shutdown_token) =
            HeartbeatStreamState::new();
        let heartbeat_tx = heartbeat_tx_source.clone();

        let join_handle = tokio::spawn(
            // The stream of incoming heartbeats uses unfold to carry state across each item in the
            // stream. The closure checks if, for any given activity, we should heartbeat or not
            // depending on its delay and when we last issued a heartbeat for it.
            futures_util::stream::unfold(heartbeat_stream_state, move |mut hb_states| {
                async move {
                    let hb = tokio::select! {
                        biased;

                        _ = hb_states.cancellation_token.cancelled() => {
                            return None
                        }
                        hb = hb_states.incoming_hbs.recv() => match hb {
                            None => return None,
                            Some(hb) => hb,
                        }
                    };

                    Some((
                        match hb {
                            HeartbeatAction::SendHeartbeat(hb) => hb_states.record(hb),
                            HeartbeatAction::CompleteReport(tt) => hb_states.handle_report_completed(tt),
                            HeartbeatAction::CompleteThrottle(tt) => hb_states.handle_throttle_completed(tt),
                            HeartbeatAction::Evict{ token, on_complete } => hb_states.evict(token, on_complete),
                        },
                        hb_states,
                    ))
                }
            })
                // Filters out `None`s
                .filter_map(|opt| async { opt })
                .for_each_concurrent(None, move |action| {
                    let heartbeat_tx = heartbeat_tx_source.clone();
                    let sg = client.clone();
                    let cancels_tx = cancels_tx.clone();
                    async move {
                        match action {
                            HeartbeatExecutorAction::Sleep(tt, duration, cancellation_token) => {
                                tokio::select! {
                                _ = cancellation_token.cancelled() => (),
                                _ = tokio::time::sleep(duration) => {
                                    let _ = heartbeat_tx.send(HeartbeatAction::CompleteThrottle(tt));
                                },
                            };
                            }
                            HeartbeatExecutorAction::Report { task_token: tt, details } => {
                                match sg
                                    .record_activity_heartbeat(tt.clone(), details.into_payloads())
                                    .await
                                {
                                    Ok(RecordActivityTaskHeartbeatResponse {
                                           cancel_requested, activity_paused, activity_reset
                                       }) => {
                                        if cancel_requested || activity_paused || activity_reset {
                                            // Prioritize Cancel / reset over pause
                                            let reason = if cancel_requested {
                                                ActivityCancelReason::Cancelled
                                            } else if activity_reset {
                                                ActivityCancelReason::Reset
                                            } else {
                                                ActivityCancelReason::Paused
                                            };
                                            cancels_tx
                                                .send(PendingActivityCancel::new(
                                                    tt.clone(),
                                                    reason,
                                                    ActivityCancellationDetails {
                                                        is_cancelled: cancel_requested,
                                                        is_paused: activity_paused,
                                                        is_reset: activity_reset,
                                                        ..Default::default()
                                                    }
                                                ))
                                                .expect(
                                                    "Receive half of heartbeat cancels not blocked",
                                                );
                                        }
                                    }
                                    // Send cancels for any activity that learns its workflow already
                                    // finished (which is one thing not found implies - other reasons
                                    // would seem equally valid).
                                    Err(s) if s.code() == tonic::Code::NotFound => {
                                        debug!(task_token = %tt,
                                           "Activity not found when recording heartbeat");
                                        cancels_tx
                                            .send(PendingActivityCancel::new(
                                                tt.clone(),
                                                ActivityCancelReason::NotFound,
                                                ActivityTask::primary_reason_to_cancellation_details(ActivityCancelReason::NotFound)
                                            ))
                                            .expect("Receive half of heartbeat cancels not blocked");
                                    }
                                    Err(e) => {
                                        warn!("Error when recording heartbeat: {:?}", e);
                                    }
                                };
                                let _ = heartbeat_tx.send(HeartbeatAction::CompleteReport(tt));
                            }
                        }
                    }
                }),
        );

        Self {
            join_handle: TakeCell::new(join_handle),
            shutdown_token,
            heartbeat_tx,
        }
    }
    /// Records a new heartbeat, the first call will result in an immediate call to the server,
    /// while rapid successive calls would accumulate for up to `delay` and then latest heartbeat
    /// details will be sent to the server.
    ///
    /// It is important that this function is never called with a task token equal to one given
    /// to [Self::evict] after it has been called, doing so will cause a memory leak, as there is
    /// no longer an efficient way to forget about that task token.
    pub(super) fn record(
        &self,
        hb: ActivityHeartbeat,
        throttle_interval: Duration,
        timeout_resetter: Option<Arc<Notify>>,
    ) -> Result<(), ActivityHeartbeatError> {
        self.heartbeat_tx
            .send(HeartbeatAction::SendHeartbeat(ValidActivityHeartbeat {
                task_token: TaskToken(hb.task_token),
                details: hb.details,
                throttle_interval,
                timeout_resetter,
            }))
            .expect("Receive half of the heartbeats event channel must not be dropped");

        Ok(())
    }

    /// Tell the heartbeat manager we are done forever with a certain task, so it may be forgotten.
    /// This will also force-flush the most recently provided details.
    /// Record *should* not be called with the same TaskToken after calling this.
    pub(super) async fn evict(&self, task_token: TaskToken) {
        let completed = Arc::new(Notify::new());
        let _ = self.heartbeat_tx.send(HeartbeatAction::Evict {
            token: task_token,
            on_complete: completed.clone(),
        });
        completed.notified().await;
    }

    /// Initiates shutdown procedure by stopping lifecycle loop and awaiting for all in-flight
    /// heartbeat requests to be flushed to the server.
    pub(super) async fn shutdown(&self) {
        self.shutdown_token.cancel();
        if let Some(h) = self.join_handle.take_once() {
            let handle_r = h.await;
            if let Err(e) = handle_r
                && !e.is_cancelled()
            {
                error!(
                    "Unexpected error joining heartbeating tasks during shutdown: {:?}",
                    e
                )
            }
        }
    }
}

#[derive(Debug)]
struct ActivityHeartbeatState {
    /// If None and throttle interval is over, untrack this task token
    last_recorded_details: Option<Vec<Payload>>,
    /// True if we've queued up a request to record against server, but it hasn't yet completed
    is_record_in_flight: bool,
    last_send_requested: Instant,
    throttle_interval: Duration,
    throttled_cancellation_token: Option<CancellationToken>,
    timeout_resetter: Option<Arc<Notify>>,
}

impl ActivityHeartbeatState {
    /// Get duration to sleep by subtracting `throttle_interval` by elapsed time since
    /// `last_send_requested`
    fn get_throttle_sleep_duration(&self) -> Duration {
        let time_since_last_sent = self.last_send_requested.elapsed();

        if time_since_last_sent > Duration::ZERO && self.throttle_interval > time_since_last_sent {
            self.throttle_interval - time_since_last_sent
        } else {
            Duration::ZERO
        }
    }
}

#[derive(Debug)]
struct HeartbeatStreamState {
    tt_to_state: HashMap<TaskToken, ActivityHeartbeatState>,
    tt_needs_flush: HashMap<TaskToken, Arc<Notify>>,
    incoming_hbs: UnboundedReceiver<HeartbeatAction>,
    /// Token that can be used to cancel the entire stream.
    /// Requests to the server are not cancelled with this token.
    cancellation_token: CancellationToken,
}

impl HeartbeatStreamState {
    fn new() -> (Self, UnboundedSender<HeartbeatAction>, CancellationToken) {
        let (heartbeat_tx, incoming_hbs) = unbounded_channel();
        let cancellation_token = CancellationToken::new();
        (
            Self {
                cancellation_token: cancellation_token.clone(),
                tt_to_state: Default::default(),
                tt_needs_flush: Default::default(),
                incoming_hbs,
            },
            heartbeat_tx,
            cancellation_token,
        )
    }

    /// Record a heartbeat received from lang
    fn record(&mut self, hb: ValidActivityHeartbeat) -> Option<HeartbeatExecutorAction> {
        match self.tt_to_state.entry(hb.task_token.clone()) {
            Entry::Vacant(e) => {
                let state = ActivityHeartbeatState {
                    throttle_interval: hb.throttle_interval,
                    last_send_requested: Instant::now(),
                    // Don't record here because we already flush out these details.
                    // None is used to mark that after throttling we can stop tracking this task
                    // token.
                    last_recorded_details: None,
                    is_record_in_flight: true,
                    throttled_cancellation_token: None,
                    timeout_resetter: hb.timeout_resetter,
                };
                e.insert(state);
                Some(HeartbeatExecutorAction::Report {
                    task_token: hb.task_token,
                    details: hb.details,
                })
            }
            Entry::Occupied(mut o) => {
                let state = o.get_mut();
                state.last_recorded_details = Some(hb.details);
                state.timeout_resetter = hb.timeout_resetter;
                None
            }
        }
    }

    /// Heartbeat report to server completed
    fn handle_report_completed(&mut self, tt: TaskToken) -> Option<HeartbeatExecutorAction> {
        if let Some(not) = self.tt_needs_flush.remove(&tt) {
            not.notify_one();
        }
        if let Some(st) = self.tt_to_state.get_mut(&tt) {
            st.is_record_in_flight = false;
            let cancellation_token = self.cancellation_token.child_token();
            st.throttled_cancellation_token = Some(cancellation_token.clone());
            if let Some(ref r) = st.timeout_resetter {
                r.notify_one();
            }
            // Always sleep for simplicity even if the duration is 0
            Some(HeartbeatExecutorAction::Sleep(
                tt.clone(),
                st.get_throttle_sleep_duration(),
                cancellation_token,
            ))
        } else {
            None
        }
    }

    /// Throttling completed, report or stop tracking task token
    fn handle_throttle_completed(&mut self, tt: TaskToken) -> Option<HeartbeatExecutorAction> {
        match self.tt_to_state.entry(tt.clone()) {
            Entry::Occupied(mut e) => {
                let state = e.get_mut();
                if let Some(details) = state.last_recorded_details.take() {
                    // Delete the recorded details before reporting
                    // Reset the cancellation token and schedule another report
                    state.throttled_cancellation_token = None;
                    state.last_send_requested = Instant::now();
                    state.is_record_in_flight = true;
                    Some(HeartbeatExecutorAction::Report {
                        task_token: tt,
                        details,
                    })
                } else {
                    // Nothing to report, forget this task token
                    e.remove();
                    None
                }
            }
            Entry::Vacant(_) => None,
        }
    }

    /// Activity should not be tracked anymore, cancel throttle timer if running.
    ///
    /// Will return a report action if there are recorded details present, to ensure we flush the
    /// latest details before we cease tracking this activity.
    fn evict(
        &mut self,
        tt: TaskToken,
        on_complete: Arc<Notify>,
    ) -> Option<HeartbeatExecutorAction> {
        if let Some(state) = self.tt_to_state.remove(&tt) {
            if let Some(cancel_tok) = state.throttled_cancellation_token {
                cancel_tok.cancel();
            }
            if let Some(last_deets) = state.last_recorded_details {
                self.tt_needs_flush.insert(tt.clone(), on_complete);
                return Some(HeartbeatExecutorAction::Report {
                    task_token: tt,
                    details: last_deets,
                });
            } else if state.is_record_in_flight {
                self.tt_needs_flush.insert(tt, on_complete);
                return None;
            }
        }
        // Since there's nothing to flush immediately report back that eviction is finished
        on_complete.notify_one();
        None
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::worker::client::mocks::mock_worker_client;
    use std::time::Duration;
    use temporal_sdk_core_protos::temporal::api::{
        common::v1::Payload, workflowservice::v1::RecordActivityTaskHeartbeatResponse,
    };
    use tokio::time::sleep;

    /// Ensure that heartbeats that are sent with a small `throttle_interval` are aggregated and sent roughly once
    /// every 1/2 of the heartbeat timeout.
    #[tokio::test]
    async fn process_heartbeats_and_shutdown() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        // Send 2 heartbeat requests for 20ms apart.
        // The first heartbeat should be sent right away, and
        // the second should be throttled until 50ms have passed.
        for i in 0_u8..2 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(50));
            sleep(Duration::from_millis(20)).await;
        }
        // sleep again to let heartbeats be flushed
        sleep(Duration::from_millis(20)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn send_heartbeats_less_frequently_than_throttle_interval() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(3);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        // Heartbeats always get sent if recorded less frequently than the throttle interval
        for i in 0_u8..3 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(10));
            sleep(Duration::from_millis(20)).await;
        }
        hm.shutdown().await;
    }

    /// Ensure that heartbeat can be called from a tight loop and correctly throttle
    #[tokio::test]
    async fn process_tight_loop_and_shutdown() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(1);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        // Send a whole bunch of heartbeats very fast. We should still only send one total.
        for i in 0_u8..50 {
            record_heartbeat(&hm, fake_task_token.clone(), i, Duration::from_millis(2000));
            // Let it propagate
            sleep(Duration::from_millis(10)).await;
        }
        hm.shutdown().await;
    }

    /// This test reports one heartbeat and waits for the throttle_interval to elapse before sending another
    #[tokio::test]
    async fn report_heartbeat_after_timeout() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        sleep(Duration::from_millis(500)).await;
        record_heartbeat(&hm, fake_task_token, 1, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(50)).await;
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn evict_works() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(2);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
        hm.evict(fake_task_token.clone().into()).await;
        record_heartbeat(&hm, fake_task_token, 0, Duration::from_millis(100));
        // Let it propagate
        sleep(Duration::from_millis(10)).await;
        // We know it works b/c otherwise we would have only called record 1 time w/o sleep
        hm.shutdown().await;
    }

    #[tokio::test]
    async fn evict_immediate_after_record() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_record_activity_heartbeat()
            .returning(|_, _| Ok(RecordActivityTaskHeartbeatResponse::default()))
            .times(1);
        let (cancel_tx, _cancel_rx) = unbounded_channel();
        let hm = ActivityHeartbeatManager::new(Arc::new(mock_client), cancel_tx);
        let fake_task_token = vec![1, 2, 3];
        record_heartbeat(&hm, fake_task_token.clone(), 0, Duration::from_millis(100));
        hm.evict(fake_task_token.clone().into()).await;
        hm.shutdown().await;
    }

    fn record_heartbeat(
        hm: &ActivityHeartbeatManager,
        task_token: Vec<u8>,
        payload_data: u8,
        throttle_interval: Duration,
    ) {
        hm.record(
            ActivityHeartbeat {
                task_token,
                details: vec![Payload {
                    metadata: Default::default(),
                    data: vec![payload_data],
                }],
            },
            // Mimic the same delay we would apply in activity task manager
            throttle_interval,
            None,
        )
        .expect("hearbeat recording should not fail");
    }
}
