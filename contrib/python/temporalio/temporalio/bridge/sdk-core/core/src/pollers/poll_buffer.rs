use crate::{
    abstractions::{ActiveCounter, MeteredPermitDealer, OwnedMeteredSemPermit, dbg_panic},
    pollers::{self, Poller},
    worker::{
        WFTPollerShared,
        client::{PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient},
    },
};
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use governor::{Quota, RateLimiter};
use std::{
    cmp,
    fmt::Debug,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporal_client::{ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT, NoRetryOnMatching};
use temporal_sdk_core_api::worker::{
    ActivitySlotKind, NexusSlotKind, PollerBehavior, SlotKind, WorkflowSlotKind,
};
use temporal_sdk_core_protos::temporal::api::{
    taskqueue::v1::PollerScalingDecision,
    workflowservice::v1::{
        PollActivityTaskQueueResponse, PollNexusTaskQueueResponse, PollWorkflowTaskQueueResponse,
    },
};
use tokio::{
    sync::{
        Mutex, broadcast,
        mpsc::{UnboundedReceiver, unbounded_channel},
        watch,
    },
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tracing::Instrument;

type PollReceiver<T, SK> =
    Mutex<UnboundedReceiver<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>>>;
pub(crate) struct LongPollBuffer<T, SK: SlotKind> {
    buffered_polls: PollReceiver<T, SK>,
    shutdown: CancellationToken,
    poller_task: JoinHandle<()>,
    /// Pollers won't actually start polling until initialized & value is sent
    starter: broadcast::Sender<()>,
    did_start: AtomicBool,
}

pub(crate) struct WorkflowTaskOptions {
    pub(crate) wft_poller_shared: Option<Arc<WFTPollerShared>>,
}

pub(crate) struct ActivityTaskOptions {
    /// Local ratelimit
    pub(crate) max_worker_acts_per_second: Option<f64>,
    /// Server ratelimit
    pub(crate) max_tps: Option<f64>,
}

impl LongPollBuffer<PollWorkflowTaskQueueResponse, WorkflowSlotKind> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_workflow_task(
        client: Arc<dyn WorkerClient>,
        task_queue: String,
        sticky_queue: Option<String>,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<WorkflowSlotKind>,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
        options: WorkflowTaskOptions,
    ) -> Self {
        let is_sticky = sticky_queue.is_some();
        let poll_scaler = PollScaler::new(poller_behavior, num_pollers_handler, shutdown.clone());
        if let Some(wftps) = options.wft_poller_shared.as_ref() {
            if is_sticky {
                wftps.set_sticky_active(poll_scaler.active_rx.clone());
            } else {
                wftps.set_non_sticky_active(poll_scaler.active_rx.clone());
            };
        }
        let pre_permit_delay = options.wft_poller_shared.clone().map(|wftps| {
            move || {
                let shared = wftps.clone();
                async move {
                    shared.wait_if_needed(is_sticky).await;
                }
            }
        });

        let post_poll_fn = options.wft_poller_shared.clone().map(|wftps| {
            move |t: &PollWorkflowTaskQueueResponse| {
                if is_sticky {
                    wftps.record_sticky_backlog(t.backlog_count_hint as usize)
                }
            }
        });
        let no_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
            Some(NoRetryOnMatching {
                predicate: poll_scaling_error_matcher,
            })
        } else {
            None
        };
        let poll_fn = move |timeout_override: Option<Duration>| {
            let client = client.clone();
            let task_queue = task_queue.clone();
            let sticky_queue_name = sticky_queue.clone();
            async move {
                client
                    .poll_workflow_task(
                        PollOptions {
                            task_queue,
                            no_retry,
                            timeout_override,
                        },
                        PollWorkflowOptions { sticky_queue_name },
                    )
                    .await
            }
        };
        Self::new(
            poll_fn,
            permit_dealer,
            shutdown,
            poll_scaler,
            pre_permit_delay,
            post_poll_fn,
        )
    }
}

impl LongPollBuffer<PollActivityTaskQueueResponse, ActivitySlotKind> {
    pub(crate) fn new_activity_task(
        client: Arc<dyn WorkerClient>,
        task_queue: String,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<ActivitySlotKind>,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
        options: ActivityTaskOptions,
    ) -> Self {
        let pre_permit_delay = options
            .max_worker_acts_per_second
            .and_then(|ps| {
                Quota::with_period(Duration::from_secs_f64(ps.recip()))
                    .map(|q| Arc::new(RateLimiter::direct(q)))
            })
            .map(|rl| {
                move || {
                    let rl = rl.clone();
                    async move { rl.until_ready().await }.boxed()
                }
            });
        let no_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
            Some(NoRetryOnMatching {
                predicate: poll_scaling_error_matcher,
            })
        } else {
            None
        };
        let poll_fn = move |timeout_override: Option<Duration>| {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move {
                client
                    .poll_activity_task(
                        PollOptions {
                            task_queue,
                            no_retry,
                            timeout_override,
                        },
                        PollActivityOptions {
                            max_tasks_per_sec: options.max_tps,
                        },
                    )
                    .await
            }
        };

        let poll_scaler = PollScaler::new(poller_behavior, num_pollers_handler, shutdown.clone());
        Self::new(
            poll_fn,
            permit_dealer,
            shutdown,
            poll_scaler,
            pre_permit_delay,
            None::<fn(&PollActivityTaskQueueResponse)>,
        )
    }
}

impl LongPollBuffer<PollNexusTaskQueueResponse, NexusSlotKind> {
    pub(crate) fn new_nexus_task(
        client: Arc<dyn WorkerClient>,
        task_queue: String,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<NexusSlotKind>,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
    ) -> Self {
        let no_retry = if matches!(poller_behavior, PollerBehavior::Autoscaling { .. }) {
            Some(NoRetryOnMatching {
                predicate: poll_scaling_error_matcher,
            })
        } else {
            None
        };
        let poll_fn = move |timeout_override: Option<Duration>| {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move {
                client
                    .poll_nexus_task(PollOptions {
                        task_queue,
                        no_retry,
                        timeout_override,
                    })
                    .await
            }
        };
        Self::new(
            poll_fn,
            permit_dealer,
            shutdown.clone(),
            PollScaler::new(poller_behavior, num_pollers_handler, shutdown),
            None::<fn() -> BoxFuture<'static, ()>>,
            None::<fn(&PollNexusTaskQueueResponse)>,
        )
    }
}

impl<T, SK> LongPollBuffer<T, SK>
where
    T: TaskPollerResult + Send + Debug + 'static,
    SK: SlotKind + 'static,
{
    fn new<FT, DelayFut, F>(
        poll_fn: impl Fn(Option<Duration>) -> FT + Send + Sync + 'static,
        permit_dealer: MeteredPermitDealer<SK>,
        shutdown: CancellationToken,
        mut poll_scaler: PollScaler<F>,
        pre_permit_delay: Option<impl Fn() -> DelayFut + Send + Sync + 'static>,
        post_poll_fn: Option<impl Fn(&T) + Send + Sync + 'static>,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
        DelayFut: Future<Output = ()> + Send,
        F: Fn(usize) + Send + Sync + 'static,
    {
        let (tx, rx) = unbounded_channel();
        let (starter, wait_for_start) = broadcast::channel(1);
        let pf = Arc::new(poll_fn);
        let post_pf = Arc::new(post_poll_fn);
        let mut wait_for_start = wait_for_start.resubscribe();
        let shutdown_clone = shutdown.clone();

        let poller_task = tokio::spawn(
            async move {
                tokio::select! {
                    _ = wait_for_start.recv() => (),
                    _ = shutdown_clone.cancelled() => return,
                }
                drop(wait_for_start);

                let (spawned_tx, spawned_rx) = unbounded_channel();
                // This is needed to ensure we don't exit this task before all received polls have
                // been sent out for processing.
                let poll_task_awaiter = tokio::spawn(async move {
                    UnboundedReceiverStream::new(spawned_rx)
                        .for_each_concurrent(None, |t| async move {
                            handle_task_panic(t).await;
                        })
                        .await;
                });
                loop {
                    if shutdown_clone.is_cancelled() {
                        break;
                    }
                    if let Some(ref ppd) = pre_permit_delay {
                        tokio::select! {
                            _ = ppd() => (),
                            _ = shutdown_clone.cancelled() => break,
                        }
                    }
                    let permit = tokio::select! {
                        p = permit_dealer.acquire_owned() => p,
                        _ = shutdown_clone.cancelled() => break,
                    };
                    let active_guard = tokio::select! {
                        ag = poll_scaler.wait_until_allowed() => ag,
                        _ = shutdown_clone.cancelled() => break,
                    };
                    // Spawn poll task
                    let shutdown = shutdown_clone.clone();
                    let pf = pf.clone();
                    let post_pf = post_pf.clone();
                    let tx = tx.clone();
                    let report_handle = poll_scaler.get_report_handle();
                    // Reduce poll timeout if we're frequently getting tasks, to avoid having many
                    // outstanding long polls for a full minute after a burst subsides.
                    let timeout_override =
                        if report_handle.ingested_this_period.load(Ordering::Relaxed) > 1 {
                            Some(Duration::from_secs(11))
                        } else {
                            None
                        };
                    let poll_task = tokio::spawn(async move {
                        let r = tokio::select! {
                            r = pf(timeout_override) => r,
                            _ = shutdown.cancelled() => return,
                        };
                        drop(active_guard);
                        if let Ok(r) = &r
                            && let Some(ppf) = post_pf.as_ref()
                        {
                            ppf(r);
                        }
                        if report_handle.poll_result(&r) {
                            let _ = tx.send(r.map(|r| (r, permit)));
                        }
                    });
                    let _ = spawned_tx.send(poll_task);
                }
                drop(spawned_tx);
                poll_task_awaiter.await.unwrap();
                if let Some(it) = poll_scaler.ingestor_task {
                    it.await.unwrap();
                }
            }
            .instrument(info_span!("polling_task").or_current()),
        );
        Self {
            buffered_polls: Mutex::new(rx),
            shutdown,
            poller_task,
            starter,
            did_start: AtomicBool::new(false),
        }
    }
}

#[async_trait::async_trait]
impl<T, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for LongPollBuffer<T, SK>
where
    T: Send + Sync + Debug + 'static,
    SK: SlotKind + 'static,
{
    /// Poll for the next item from this poller
    ///
    /// Returns `None` if the poller has been shut down
    #[instrument(name = "long_poll", level = "trace", skip(self))]
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>> {
        if !self.did_start.fetch_or(true, Ordering::Relaxed) {
            let _ = self.starter.send(());
        }

        let mut locked = self.buffered_polls.lock().await;
        (*locked).recv().await
    }

    fn notify_shutdown(&self) {
        self.shutdown.cancel();
    }

    async fn shutdown(mut self) {
        self.notify_shutdown();
        handle_task_panic(self.poller_task).await;
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
    }
}

async fn handle_task_panic(t: JoinHandle<()>) {
    if let Err(e) = t.await
        && e.is_panic()
    {
        let as_panic = e.into_panic().downcast::<String>();
        dbg_panic!(
            "Poller task died or did not terminate cleanly: {:?}",
            as_panic
        );
    }
}

/// The PollScaler is responsible for managing the number of pollers based on the current load.
///
/// It does so by receiving suggestions from the server about whether to scale up or down. It will
/// always respect scale down decisions (until the number of pollers reaches the minimum), but may
/// choose to ignore scale up decisions if it appears that adding more pollers is not contributing
/// to increased task throughput. See more detailed comments in the implementation.
struct PollScaler<F> {
    report_handle: Arc<PollScalerReportHandle>,
    active_tx: watch::Sender<usize>,
    active_rx: watch::Receiver<usize>,
    num_pollers_handler: Option<Arc<F>>,
    ingestor_task: Option<JoinHandle<()>>,
}

impl<F> PollScaler<F>
where
    F: Fn(usize),
{
    fn new(
        behavior: PollerBehavior,
        num_pollers_handler: Option<F>,
        shutdown: CancellationToken,
    ) -> Self {
        let (active_tx, active_rx) = watch::channel(0);
        let num_pollers_handler = num_pollers_handler.map(Arc::new);
        let (min, max, target) = match behavior {
            PollerBehavior::SimpleMaximum(m) => (1, m, m),
            PollerBehavior::Autoscaling {
                minimum,
                maximum,
                initial,
            } => (minimum, maximum, initial),
        };
        let report_handle = Arc::new(PollScalerReportHandle {
            max,
            min,
            target: AtomicUsize::new(target),
            ever_saw_scaling_decision: AtomicBool::default(),
            behavior,
            ingested_this_period: Default::default(),
            ingested_last_period: Default::default(),
            scale_up_allowed: AtomicBool::new(true),
        });
        let rhc = report_handle.clone();
        let ingestor_task = if behavior.is_autoscaling() {
            // Here we spawn a task to periodically check if we should permit increasing the
            // poller count further. We do this by comparing the number of ingested items in the
            // current period with the number of ingested items in the previous period. If we
            // are successfully ingesting more items, then it makes sense to allow scaling up.
            // If we aren't, then we're probably limited by how fast we can process the tasks
            // and it's not worth increasing the poller count further.
            Some(tokio::task::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {}
                        _ = shutdown.cancelled() => { break; }
                    }
                    let ingested = rhc.ingested_this_period.swap(0, Ordering::Relaxed);
                    let ingested_last = rhc.ingested_last_period.swap(ingested, Ordering::Relaxed);
                    rhc.scale_up_allowed.store(
                        ingested >= (ingested_last as f64 * 1.1) as usize,
                        Ordering::Relaxed,
                    );
                }
            }))
        } else {
            None
        };
        Self {
            report_handle,
            active_tx,
            active_rx,
            num_pollers_handler,
            ingestor_task,
        }
    }

    async fn wait_until_allowed(&mut self) -> ActiveCounter<impl Fn(usize) + use<F>> {
        self.active_rx
            .wait_for(|v| {
                *v < self.report_handle.max
                    && *v < self.report_handle.target.load(Ordering::Relaxed)
            })
            .await
            .expect("Poll allow does not panic");
        ActiveCounter::new(self.active_tx.clone(), self.num_pollers_handler.clone())
    }

    fn get_report_handle(&self) -> Arc<PollScalerReportHandle> {
        self.report_handle.clone()
    }
}

struct PollScalerReportHandle {
    max: usize,
    min: usize,
    target: AtomicUsize,
    ever_saw_scaling_decision: AtomicBool,
    behavior: PollerBehavior,

    ingested_this_period: AtomicUsize,
    ingested_last_period: AtomicUsize,
    scale_up_allowed: AtomicBool,
}

impl PollScalerReportHandle {
    /// Returns true if the response should be passed on, false if it should be swallowed
    fn poll_result(&self, res: &Result<impl TaskPollerResult, tonic::Status>) -> bool {
        match res {
            Ok(res) => {
                if let PollerBehavior::SimpleMaximum(_) = self.behavior {
                    // We don't do auto-scaling with the simple max
                    return true;
                }
                if !res.is_empty() {
                    self.ingested_this_period.fetch_add(1, Ordering::Relaxed);
                }
                if let Some(scaling_decision) = res.scaling_decision() {
                    match scaling_decision.poll_request_delta_suggestion.cmp(&0) {
                        cmp::Ordering::Less => self.change_target(
                            usize::saturating_sub,
                            scaling_decision
                                .poll_request_delta_suggestion
                                .unsigned_abs() as usize,
                        ),
                        cmp::Ordering::Greater => {
                            // Only allow scale up if in the last period it increased ingestion rate
                            if self.scale_up_allowed.load(Ordering::Relaxed) {
                                self.change_target(
                                    usize::saturating_add,
                                    scaling_decision.poll_request_delta_suggestion as usize,
                                )
                            }
                        }
                        cmp::Ordering::Equal => {}
                    }
                    self.ever_saw_scaling_decision
                        .store(true, Ordering::Relaxed);
                }
                // We want to avoid scaling down on empty polls if the server has never made any
                // scaling decisions - otherwise we might never scale up again.
                else if self.ever_saw_scaling_decision.load(Ordering::Relaxed) && res.is_empty() {
                    self.change_target(usize::saturating_sub, 1);
                }
            }
            Err(e) => {
                if matches!(self.behavior, PollerBehavior::Autoscaling { .. }) {
                    // We should only react to errors in autoscaling mode if we saw a scaling
                    // decision
                    if self.ever_saw_scaling_decision.load(Ordering::Relaxed) {
                        debug!("Got error from server while polling: {:?}", e);
                        if e.code() == Code::ResourceExhausted {
                            // Scale down significantly for resource exhaustion
                            self.change_target(usize::saturating_div, 2);
                        } else {
                            // Other codes that would normally have made us back off briefly can
                            // reclaim this poller
                            self.change_target(usize::saturating_sub, 1);
                        }
                    }
                    // Only propagate errors out if they weren't because of the short-circuiting
                    // logic. IE: We don't want to fail callers because we said we wanted to know
                    // about ResourceExhausted errors, but we haven't seen a scaling decision yet,
                    // so we're not reacting to errors, only propagating them.
                    return !e
                        .metadata()
                        .contains_key(ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT);
                }
            }
        }
        true
    }

    #[inline]
    fn change_target(&self, change: fn(usize, usize) -> usize, change_by: usize) {
        self.target
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(change(v, change_by).clamp(self.min, self.max))
            })
            .expect("Cannot fail because always returns Some");
    }
}

/// A poller capable of polling on a sticky and a nonsticky queue simultaneously for workflow tasks.
#[derive(derive_more::Constructor)]
pub(crate) struct WorkflowTaskPoller {
    normal_poller: PollWorkflowTaskBuffer,
    sticky_poller: Option<PollWorkflowTaskBuffer>,
}
type PollWorkflowTaskBuffer = LongPollBuffer<PollWorkflowTaskQueueResponse, WorkflowSlotKind>;

#[async_trait::async_trait]
impl
    Poller<(
        PollWorkflowTaskQueueResponse,
        OwnedMeteredSemPermit<WorkflowSlotKind>,
    )> for WorkflowTaskPoller
{
    async fn poll(
        &self,
    ) -> Option<
        pollers::Result<(
            PollWorkflowTaskQueueResponse,
            OwnedMeteredSemPermit<WorkflowSlotKind>,
        )>,
    > {
        if let Some(sq) = self.sticky_poller.as_ref() {
            tokio::select! {
                r = self.normal_poller.poll() => r,
                r = sq.poll() => r,
            }
        } else {
            self.normal_poller.poll().await
        }
    }

    fn notify_shutdown(&self) {
        self.normal_poller.notify_shutdown();
        if let Some(sq) = self.sticky_poller.as_ref() {
            sq.notify_shutdown();
        }
    }

    async fn shutdown(mut self) {
        self.normal_poller.shutdown().await;
        if let Some(sq) = self.sticky_poller {
            sq.shutdown().await;
        }
    }

    async fn shutdown_box(self: Box<Self>) {
        let this = *self;
        this.shutdown().await;
    }
}

/// Returns true for errors that the poller scaler wants to see
fn poll_scaling_error_matcher(err: &tonic::Status) -> bool {
    matches!(
        err.code(),
        Code::ResourceExhausted | Code::Cancelled | Code::DeadlineExceeded
    )
}

pub(crate) trait TaskPollerResult {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision>;
    /// Returns true if this is an empty poll response (IE: poll timeout)
    fn is_empty(&self) -> bool;
}
impl TaskPollerResult for PollWorkflowTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}
impl TaskPollerResult for PollActivityTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}
impl TaskPollerResult for PollNexusTaskQueueResponse {
    fn scaling_decision(&self) -> Option<&PollerScalingDecision> {
        self.poller_scaling_decision.as_ref()
    }
    fn is_empty(&self) -> bool {
        self.task_token.is_empty()
    }
}

#[cfg(test)]
#[derive(derive_more::Constructor)]
pub(crate) struct MockPermittedPollBuffer<PT, SK: SlotKind> {
    sem: Arc<MeteredPermitDealer<SK>>,
    inner: PT,
}

#[cfg(test)]
#[async_trait::async_trait]
impl<T, PT, SK> Poller<(T, OwnedMeteredSemPermit<SK>)> for MockPermittedPollBuffer<PT, SK>
where
    T: Send + Sync + 'static,
    PT: Poller<T> + Send + Sync + 'static,
    SK: SlotKind + 'static,
{
    async fn poll(&self) -> Option<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>> {
        let p = self.sem.acquire_owned().await;
        self.inner.poll().await.map(|r| r.map(|r| (r, p)))
    }

    fn notify_shutdown(&self) {
        self.inner.notify_shutdown();
    }

    async fn shutdown(self) {
        self.inner.shutdown().await;
    }

    async fn shutdown_box(self: Box<Self>) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::tests::fixed_size_permit_dealer,
        worker::client::mocks::mock_manual_worker_client,
    };
    use futures_util::FutureExt;
    use std::time::Duration;
    use tokio::select;

    #[tokio::test]
    async fn only_polls_once_with_1_poller() {
        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_poll_workflow_task()
            .times(2)
            .returning(move |_, _| {
                async {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    Ok(Default::default())
                }
                .boxed()
            });

        let pb = LongPollBuffer::new_workflow_task(
            Arc::new(mock_client),
            "sometq".to_string(),
            None,
            PollerBehavior::SimpleMaximum(1),
            fixed_size_permit_dealer(10),
            CancellationToken::new(),
            None::<fn(usize)>,
            WorkflowTaskOptions {
                wft_poller_shared: Some(Arc::new(WFTPollerShared::new(Some(10)))),
            },
        );

        // Poll a bunch of times, "interrupting" it each time, we should only actually have polled
        // once since the poll takes a while
        let mut last_val = false;
        for _ in 0..10 {
            select! {
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
                    last_val = true;
                }
                _ = pb.poll() => {
                }
            }
        }
        assert!(last_val);
        // Now we grab the buffered poll response, the poll task will go again but we don't grab it,
        // therefore we will have only polled twice.
        pb.poll().await.unwrap().unwrap();
        pb.shutdown().await;
    }

    #[tokio::test]
    async fn autoscale_wont_fail_caller_on_short_circuited_error() {
        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_poll_workflow_task()
            .times(1)
            .returning(move |_, _| {
                async {
                    let mut st = tonic::Status::cancelled("whatever");
                    st.metadata_mut()
                        .insert(ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT, 1.into());
                    Err(st)
                }
                .boxed()
            });
        mock_client
            .expect_poll_workflow_task()
            .returning(move |_, _| async { Ok(Default::default()) }.boxed());

        let pb = LongPollBuffer::new_workflow_task(
            Arc::new(mock_client),
            "sometq".to_string(),
            None,
            PollerBehavior::Autoscaling {
                minimum: 1,
                maximum: 1,
                initial: 1,
            },
            fixed_size_permit_dealer(1),
            CancellationToken::new(),
            None::<fn(usize)>,
            WorkflowTaskOptions {
                wft_poller_shared: Some(Arc::new(WFTPollerShared::new(Some(1)))),
            },
        );

        // Should not see error, unwraps should get empty response
        pb.poll().await.unwrap().unwrap();
        pb.shutdown().await;
    }
}
