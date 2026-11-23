use crate::{
    abstractions::{ActiveCounter, MeteredPermitDealer, OwnedMeteredSemPermit, dbg_panic},
    pollers::{self, Poller},
    worker::{
        ActivitySlotKind, NamespaceCapabilities, NexusSlotKind, PollerBehavior, SlotKind,
        WFTPollerShared, WorkflowSlotKind,
        client::{
            PollActivityOptions, PollNexusOptions, PollOptions, PollWorkflowOptions, WorkerClient,
        },
    },
};
use backon::{BackoffBuilder, ExponentialBuilder};
use crossbeam_utils::atomic::AtomicCell;
use futures_util::{FutureExt, StreamExt, future::BoxFuture};
use std::{
    cmp,
    fmt::Debug,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporalio_client::{
    ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT, jittered, request_extensions::NoRetryOnMatching,
};
use temporalio_common::protos::temporal::api::{
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

// Mirror the retry client's `RetryOptions::task_poll_retry_policy` and
// `throttle_retry_policy` so poller autoscaling backs off on the same schedule.
// Jitter is applied via `jittered` rather than backon's `with_jitter` to keep the
// symmetric +/- factor behavior the retry client uses.
const POLL_BACKOFF_RANDOMIZATION: f64 = 0.2;
const TASK_POLL_BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_millis(200))
    .with_factor(2.0)
    .with_max_delay(Duration::from_secs(10))
    .without_max_times();
const THROTTLE_POLL_BACKOFF: ExponentialBuilder = ExponentialBuilder::new()
    .with_min_delay(Duration::from_secs(1))
    .with_factor(2.0)
    .with_max_delay(Duration::from_secs(10))
    .without_max_times();

type PollReceiver<T, SK> =
    Mutex<UnboundedReceiver<pollers::Result<(T, OwnedMeteredSemPermit<SK>)>>>;

struct PollRateLimiter {
    interval: Duration,
    next_allowed_at: Mutex<Instant>,
}

impl PollRateLimiter {
    fn new(polls_per_second: f64) -> Self {
        Self {
            interval: Duration::from_secs_f64(polls_per_second.recip()),
            next_allowed_at: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self) {
        let scheduled_at = {
            let mut next_allowed_at = self.next_allowed_at.lock().await;
            let now = Instant::now();
            let scheduled_at = (*next_allowed_at).max(now);
            *next_allowed_at = scheduled_at + self.interval;
            scheduled_at
        };
        tokio::time::sleep_until(scheduled_at.into()).await;
    }
}

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
        last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
        capabilities: Arc<NamespaceCapabilities>,
    ) -> Self {
        let is_sticky = sticky_queue.is_some();
        let poll_scaler = PollScaler::new(
            poller_behavior,
            num_pollers_handler,
            shutdown.clone(),
            last_successful_poll_time,
            capabilities.clone(),
        );
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
            capabilities,
        )
    }
}

impl LongPollBuffer<PollActivityTaskQueueResponse, ActivitySlotKind> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_activity_task(
        client: Arc<dyn WorkerClient>,
        task_queue: String,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<ActivitySlotKind>,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
        options: ActivityTaskOptions,
        last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
        capabilities: Arc<NamespaceCapabilities>,
    ) -> Self {
        let pre_permit_delay = options
            .max_worker_acts_per_second
            .map(|ps| Arc::new(PollRateLimiter::new(ps)))
            .map(|rl| {
                move || {
                    let rl = rl.clone();
                    async move { rl.wait().await }.boxed()
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

        let poll_scaler = PollScaler::new(
            poller_behavior,
            num_pollers_handler,
            shutdown.clone(),
            last_successful_poll_time,
            capabilities.clone(),
        );
        Self::new(
            poll_fn,
            permit_dealer,
            shutdown,
            poll_scaler,
            pre_permit_delay,
            None::<fn(&PollActivityTaskQueueResponse)>,
            capabilities,
        )
    }
}

impl LongPollBuffer<PollNexusTaskQueueResponse, NexusSlotKind> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_nexus_task(
        client: Arc<dyn WorkerClient>,
        task_queue: String,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<NexusSlotKind>,
        shutdown: CancellationToken,
        num_pollers_handler: Option<impl Fn(usize) + Send + Sync + 'static>,
        last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
        capabilities: Arc<NamespaceCapabilities>,
        worker_commands_queue: bool,
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
                    .poll_nexus_task(
                        PollOptions {
                            task_queue,
                            no_retry,
                            timeout_override,
                        },
                        PollNexusOptions {
                            worker_commands_queue,
                        },
                    )
                    .await
            }
        };
        Self::new(
            poll_fn,
            permit_dealer,
            shutdown.clone(),
            PollScaler::new(
                poller_behavior,
                num_pollers_handler,
                shutdown,
                last_successful_poll_time,
                capabilities.clone(),
            ),
            None::<fn() -> BoxFuture<'static, ()>>,
            None::<fn(&PollNexusTaskQueueResponse)>,
            capabilities,
        )
    }
}

// Simple way to test this w/o plumbing options through. This will be removed after
// the proper server implementation for terminating polls on worker shutdown exists.
#[cfg(test)]
use std::cell::RefCell;
#[cfg(test)]
thread_local! {
    static POLL_SHUTDOWN_INTERRUPT: RefCell<Option<Duration>> = RefCell::default();
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
        capabilities: Arc<NamespaceCapabilities>,
    ) -> Self
    where
        FT: Future<Output = pollers::Result<T>> + Send,
        DelayFut: Future<Output = ()> + Send,
        F: Fn(usize) + Send + Sync + 'static,
    {
        let (tx, rx) = unbounded_channel();
        let (starter, mut wait_for_start) = broadcast::channel(1);
        let pf = Arc::new(poll_fn);
        let post_pf = Arc::new(post_poll_fn);
        let shutdown_clone = shutdown.clone();
        #[cfg(test)]
        let poll_shutdown_interrupt_wait = POLL_SHUTDOWN_INTERRUPT.with(|v| *v.borrow());
        #[cfg(not(test))]
        let poll_shutdown_interrupt_wait =
            std::env::var("TEMPORAL_POLL_SHUTDOWN_INTERRUPT_WAIT_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map(Duration::from_millis);

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
                    let capabilities = capabilities.clone();
                    let poll_task = tokio::spawn(async move {
                        let r = if capabilities.graceful_poll_shutdown() {
                            pf(timeout_override).await
                        } else {
                            let poll_interruptor = shutdown.cancelled().then(|_| async move {
                                if let Some(w) = poll_shutdown_interrupt_wait {
                                    tokio::time::sleep(w).await;
                                }
                            });
                            tokio::select! {
                                r = pf(timeout_override) => r,
                                _ = poll_interruptor => return,
                            }
                        };
                        if let Ok(r) = &r
                            && let Some(ppf) = post_pf.as_ref()
                        {
                            ppf(r);
                        }
                        let (should_forward, backoff_duration) = report_handle.poll_result(&r);
                        if let Some(duration) = backoff_duration {
                            // Apply backoff BEFORE dropping active_guard to prevent next poll from
                            // starting
                            tokio::select! {
                                _ = tokio::time::sleep(duration) => return,
                                _ = shutdown.cancelled() => (),
                            };
                        }
                        drop(active_guard);
                        if should_forward {
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
        last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,
        capabilities: Arc<NamespaceCapabilities>,
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
            capabilities,
            behavior,
            ingested_this_period: Default::default(),
            ingested_last_period: Default::default(),
            scale_up_allowed: AtomicBool::new(true),
            last_successful_poll_time,
            exponential_backoff: parking_lot::Mutex::new(TASK_POLL_BACKOFF.build()),
            resource_exhausted_backoff: parking_lot::Mutex::new(THROTTLE_POLL_BACKOFF.build()),
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
    capabilities: Arc<NamespaceCapabilities>,
    behavior: PollerBehavior,

    ingested_this_period: AtomicUsize,
    ingested_last_period: AtomicUsize,
    scale_up_allowed: AtomicBool,
    last_successful_poll_time: Arc<AtomicCell<Option<SystemTime>>>,

    // Exponential backoff for normal errors and resource exhausted errors
    exponential_backoff: parking_lot::Mutex<backon::ExponentialBackoff>,
    resource_exhausted_backoff: parking_lot::Mutex<backon::ExponentialBackoff>,
}

impl PollScalerReportHandle {
    /// Returns (should_forward, backoff_duration)
    /// - should_forward: true if the response should be passed on, false if it should be swallowed
    /// - backoff_duration: Some(duration) if we should sleep before the next poll
    fn poll_result(
        &self,
        res: &Result<impl TaskPollerResult, tonic::Status>,
    ) -> (bool, Option<Duration>) {
        match res {
            Ok(res) => {
                self.last_successful_poll_time
                    .store(Some(SystemTime::now()));

                // Reset backoff on successful poll
                *self.exponential_backoff.lock() = TASK_POLL_BACKOFF.build();
                *self.resource_exhausted_backoff.lock() = THROTTLE_POLL_BACKOFF.build();

                if let PollerBehavior::SimpleMaximum(_) = self.behavior {
                    // We don't do auto-scaling with the simple max
                    return (true, None);
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
                } else if self.can_scale_down() && res.is_empty() {
                    self.change_target(usize::saturating_sub, 1);
                }
            }
            Err(e) => {
                if matches!(self.behavior, PollerBehavior::Autoscaling { .. }) {
                    // Follow the same backoff logic as the retry client
                    let mut backoff_duration = self
                        .exponential_backoff
                        .lock()
                        .next()
                        .map(|d| jittered(d, POLL_BACKOFF_RANDOMIZATION));
                    if e.code() == Code::ResourceExhausted {
                        backoff_duration = self
                            .resource_exhausted_backoff
                            .lock()
                            .next()
                            .map(|d| jittered(d, POLL_BACKOFF_RANDOMIZATION));
                    };

                    // Only propagate errors out if they weren't because of the short-circuiting
                    // logic. IE: We don't want to fail callers because we said we wanted to know
                    // about ResourceExhausted errors, but we haven't seen a scaling decision yet,
                    // so we're not reacting to errors, only propagating them.
                    let should_forward = !e
                        .metadata()
                        .contains_key(ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT);

                    if self.can_scale_down() {
                        debug!("Got error from server while polling: {:?}", e);
                        if e.code() == Code::ResourceExhausted {
                            // Scale down significantly for resource exhaustion
                            self.change_target(usize::saturating_div, 2);
                        } else {
                            // Other codes that would normally have made us back off briefly can reclaim this poller
                            self.change_target(usize::saturating_sub, 1);
                        }
                    }
                    return (should_forward, backoff_duration);
                }
            }
        }
        (true, None)
    }

    #[inline]
    fn change_target(&self, change: fn(usize, usize) -> usize, change_by: usize) {
        self.target
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(change(v, change_by).clamp(self.min, self.max))
            })
            .expect("Cannot fail because always returns Some");
    }

    /// We want to avoid scaling down on empty polls if the server has never made any scaling
    /// decisions - otherwise we might never scale up again. If the server supports poller
    /// autoscaling, it's safe to scale down without having seen a decision.
    fn can_scale_down(&self) -> bool {
        self.ever_saw_scaling_decision.load(Ordering::Relaxed)
            || self.capabilities.poller_autoscaling()
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

#[cfg(any(feature = "test-utilities", test))]
#[derive(derive_more::Constructor)]
pub(crate) struct MockPermittedPollBuffer<PT, SK: SlotKind> {
    sem: Arc<MeteredPermitDealer<SK>>,
    inner: PT,
}

#[cfg(any(feature = "test-utilities", test))]
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
    use rstest::rstest;
    use std::time::Duration;
    use temporalio_common::protos::temporal::api::namespace::v1::namespace_info::Capabilities;
    use tokio::{select, sync::Notify};

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
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
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
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        );

        // Should not see error, unwraps should get empty response
        pb.poll().await.unwrap().unwrap();
        pb.shutdown().await;
    }

    #[tokio::test]
    async fn poll_shutdown_waits_interrupt_period_before_cancelling() {
        POLL_SHUTDOWN_INTERRUPT.with(|v| {
            *v.borrow_mut() = Some(Duration::from_millis(200));
        });

        let mut mock_client = mock_manual_worker_client();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        let second_task_complete = Arc::new(Notify::new());
        let second_task_complete_clone = second_task_complete.clone();
        let third_task_complete = Arc::new(Notify::new());
        let third_task_complete_clone = third_task_complete.clone();

        let second_started = Arc::new(Notify::new());
        let second_started_clone = second_started.clone();
        let third_started = Arc::new(Notify::new());
        let third_started_clone = third_started.clone();

        mock_client
            .expect_poll_workflow_task()
            .returning(move |_, _| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                let second_complete = second_task_complete_clone.clone();
                let third_complete = third_task_complete_clone.clone();
                let second_started = second_started_clone.clone();
                let third_started = third_started_clone.clone();

                async move {
                    match count {
                        0 => Ok(PollWorkflowTaskQueueResponse {
                            task_token: vec![1],
                            ..Default::default()
                        }),
                        1 => {
                            second_started.notify_one();
                            second_complete.notified().await;
                            Ok(PollWorkflowTaskQueueResponse {
                                task_token: vec![2],
                                ..Default::default()
                            })
                        }
                        _ => {
                            third_started.notify_one();
                            third_complete.notified().await;
                            Ok(PollWorkflowTaskQueueResponse {
                                task_token: vec![3],
                                ..Default::default()
                            })
                        }
                    }
                }
                .boxed()
            });

        let shutdown_token = CancellationToken::new();
        let pb = LongPollBuffer::new_workflow_task(
            Arc::new(mock_client),
            "sometq".to_string(),
            None,
            PollerBehavior::SimpleMaximum(3),
            fixed_size_permit_dealer(10),
            shutdown_token.clone(),
            None::<fn(usize)>,
            WorkflowTaskOptions {
                wft_poller_shared: Some(Arc::new(WFTPollerShared::new(Some(10)))),
            },
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        );

        let first_task = pb.poll().await.expect("Should get first task");
        assert!(first_task.is_ok());
        assert_eq!(first_task.unwrap().0.task_token, vec![1]);

        // Wait for both second and third polls to start
        second_started.notified().await;
        third_started.notified().await;

        // Now both polls 2 and 3 are in progress. Trigger shutdown.
        let shutdown_time = std::time::Instant::now();
        shutdown_token.cancel();

        // Immediately unlock the second task so it completes quickly (within interrupt wait)
        second_task_complete.notify_one();

        // Second task should be received because it completes within the interrupt wait period
        let (task, _) = pb.poll().await.unwrap().unwrap();
        assert_eq!(task.task_token, vec![2]);

        // Don't notify third_task_complete - the third poll will wait for the interrupt period
        // and then be interrupted. Should return None.
        let third_task_result = pb.poll().await;
        let elapsed = shutdown_time.elapsed();

        assert!(
            third_task_result.is_none(),
            "Third task should not be received - poll should be interrupted after interrupt period"
        );

        // Total elapsed time should be at least the interrupt wait (200ms), but not significantly
        // longer.
        assert!(
            elapsed >= Duration::from_millis(200),
            "Should wait at least the interrupt period. Elapsed: {elapsed:?}",
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "Should not wait too long. Elapsed: {elapsed:?}",
        );

        // Clean up
        POLL_SHUTDOWN_INTERRUPT.with(|v| {
            *v.borrow_mut() = None;
        });
    }

    #[rstest]
    #[case::resource_exhausted(Code::ResourceExhausted)]
    #[case::internal(Code::Internal)]
    #[tokio::test]
    async fn autoscaler_applies_backoff_on_errors(#[case] error_code: Code) {
        use temporalio_common::protos::temporal::api::taskqueue::v1::PollerScalingDecision;

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();
        let first_poll_done = Arc::new(AtomicBool::new(false));
        let first_poll_done_clone = first_poll_done.clone();

        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_poll_workflow_task()
            .returning(move |_, _| {
                call_count_clone.fetch_add(1, Ordering::SeqCst);
                let first_done = first_poll_done_clone.clone();
                async move {
                    // First poll: return empty response with scaling decision
                    // This sets ever_saw_scaling_decision to true
                    if !first_done.swap(true, Ordering::SeqCst) {
                        Ok(PollWorkflowTaskQueueResponse {
                            task_token: vec![], // Empty poll
                            poller_scaling_decision: Some(PollerScalingDecision {
                                // Aggressively scale up to max
                                poll_request_delta_suggestion: 0,
                            }),
                            ..Default::default()
                        })
                    } else {
                        // All subsequent polls fail with a grpc error
                        Err(tonic::Status::new(
                            error_code,
                            format!("simulated grpc error {error_code}"),
                        ))
                    }
                }
                .boxed()
            });

        let pb = Arc::new(LongPollBuffer::new_workflow_task(
            Arc::new(mock_client),
            "sometq".to_string(),
            None,
            PollerBehavior::Autoscaling {
                minimum: 5,
                maximum: 100,
                initial: 10,
            },
            fixed_size_permit_dealer(10),
            CancellationToken::new(),
            None::<fn(usize)>,
            WorkflowTaskOptions {
                wft_poller_shared: Some(Arc::new(WFTPollerShared::new(Some(10)))),
            },
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::default()),
        ));

        // Trigger the first poll to initialize and get the scaling decision
        let pb_clone = pb.clone();
        tokio::spawn(async move {
            let _ = pb_clone.poll().await;
        });

        // Wait for the first poll to complete
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Let the hot loop run for 100ms while we continue to attempt consuming
        tokio::time::sleep(Duration::from_millis(100)).await;
        let hot_loop_calls = call_count.load(Ordering::SeqCst);

        // Without backoff, this was producing ~6300 polls in ~100ms on my machine.
        // With exponential backoff, I'm getting exactly 10 (initial poller count).
        assert!(
            hot_loop_calls == 10,
            "Expected proper backoff with == 10 polls in 100ms, but got {} polls.",
            hot_loop_calls
        );

        Arc::try_unwrap(pb)
            .unwrap_or_else(|_| panic!("Failed to unwrap Arc"))
            .shutdown()
            .await;
    }

    #[rstest]
    #[case::graceful(true)]
    #[case::legacy(false)]
    #[tokio::test]
    async fn inflight_poll_survives_shutdown_only_when_graceful(#[case] graceful: bool) {
        let mut mock_client = mock_manual_worker_client();
        let task_started = Arc::new(Notify::new());
        let task_started_clone = task_started.clone();
        let task_complete = Arc::new(Notify::new());
        let task_complete_clone = task_complete.clone();
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = call_count.clone();

        mock_client
            .expect_poll_workflow_task()
            .returning(move |_, _| {
                let count = call_count_clone.fetch_add(1, Ordering::SeqCst);
                let started = task_started_clone.clone();
                let complete = task_complete_clone.clone();
                async move {
                    match count {
                        0 => Ok(PollWorkflowTaskQueueResponse {
                            task_token: vec![1],
                            ..Default::default()
                        }),
                        _ => {
                            started.notify_one();
                            complete.notified().await;
                            Ok(PollWorkflowTaskQueueResponse {
                                task_token: vec![2],
                                ..Default::default()
                            })
                        }
                    }
                }
                .boxed()
            });

        let shutdown_token = CancellationToken::new();
        let pb = LongPollBuffer::new_workflow_task(
            Arc::new(mock_client),
            "sometq".to_string(),
            None,
            PollerBehavior::SimpleMaximum(1),
            fixed_size_permit_dealer(10),
            shutdown_token.clone(),
            None::<fn(usize)>,
            WorkflowTaskOptions {
                wft_poller_shared: None,
            },
            Arc::new(AtomicCell::new(None)),
            Arc::new(NamespaceCapabilities::resolved(Capabilities {
                worker_poll_complete_on_shutdown: graceful,
                ..Default::default()
            })),
        );

        let first = pb.poll().await.unwrap().unwrap();
        assert_eq!(first.0.task_token, vec![1]);

        // Wait for second poll to be in-flight
        task_started.notified().await;

        shutdown_token.cancel();

        if graceful {
            // Release the poll — simulates server returning empty after ShutdownWorker
            task_complete.notify_one();

            // Graceful: in-flight poll survives, second task is received
            let second = tokio::time::timeout(Duration::from_secs(2), pb.poll())
                .await
                .expect("graceful poll should complete")
                .unwrap()
                .unwrap();
            assert_eq!(second.0.task_token, vec![2]);
        } else {
            // Don't release — the poll should be killed by shutdown token before completing.
            // Buffer drains to None because the killed poll sends no result.
            let result = tokio::time::timeout(Duration::from_secs(2), pb.poll())
                .await
                .expect("legacy poll should resolve quickly");
            assert!(
                result.is_none(),
                "Legacy shutdown should kill in-flight poll, buffer returns None"
            );
        }

        pb.shutdown().await;
    }

    #[rstest]
    #[case::with_capability(true, 1, 1)]
    #[case::without_capability(false, 1, 10)]
    #[case::clamps_to_nonone_min(true, 3, 3)]
    #[test]
    fn autoscale_down_on_timeout_respects_server_capability(
        #[case] supports_autoscaling: bool,
        #[case] minimum: usize,
        #[case] expected_target: usize,
    ) {
        let handle = Arc::new(PollScalerReportHandle {
            max: 10,
            min: minimum,
            target: AtomicUsize::new(10),
            ever_saw_scaling_decision: AtomicBool::new(false),
            capabilities: Arc::new(NamespaceCapabilities::resolved(Capabilities {
                poller_autoscaling: supports_autoscaling,
                ..Default::default()
            })),
            behavior: PollerBehavior::Autoscaling {
                minimum,
                maximum: 10,
                initial: 10,
            },
            ingested_this_period: Default::default(),
            ingested_last_period: Default::default(),
            scale_up_allowed: AtomicBool::new(true),
            last_successful_poll_time: Arc::new(AtomicCell::new(None)),
            exponential_backoff: parking_lot::Mutex::new(TASK_POLL_BACKOFF.build()),
            resource_exhausted_backoff: parking_lot::Mutex::new(THROTTLE_POLL_BACKOFF.build()),
        });

        for _ in 0..20 {
            let empty_resp: Result<PollWorkflowTaskQueueResponse, tonic::Status> =
                Ok(PollWorkflowTaskQueueResponse::default());
            handle.poll_result(&empty_resp);
        }

        assert_eq!(handle.target.load(Ordering::Relaxed), expected_target);
        assert!(!handle.ever_saw_scaling_decision.load(Ordering::Relaxed));
    }
}
