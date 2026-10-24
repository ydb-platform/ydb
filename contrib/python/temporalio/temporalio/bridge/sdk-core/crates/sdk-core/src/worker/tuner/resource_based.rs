use crate::worker::{
    ActivitySlotKind, LocalActivitySlotKind, NexusSlotKind, SlotInfo, SlotInfoTrait, SlotKind,
    SlotKindType, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierPermit, WorkerTuner, WorkflowSlotKind,
};
use crossbeam_utils::atomic::AtomicCell;
use parking_lot::Mutex;
use std::{
    fs,
    marker::PhantomData,
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};
use temporalio_common::telemetry::metrics::{GaugeF64, MetricAttributes, TemporalMeter};
use tokio::{sync::watch, task::JoinHandle};

/// Implements [crate::worker::WorkerTuner] and attempts to maintain certain levels of resource
/// usage when under load.
///
/// It does so by using two PID controllers, one for memory and one for CPU, which are fed the
/// current usage levels of their respective resource as measurements. The user specifies a target
/// threshold for each, and slots are handed out if the output of both PID controllers is above some
/// defined threshold. See [ResourceBasedSlotsOptions] for the default PID controller settings.
///
/// Note that different slot types make decisions independently of each other, though they use the
/// same underlying system information.
pub struct ResourceBasedTuner<MI> {
    slots: Arc<ResourceController<MI>>,
    wf_opts: Option<ResourceSlotOptions>,
    act_opts: Option<ResourceSlotOptions>,
    la_opts: Option<ResourceSlotOptions>,
    nexus_opts: Option<ResourceSlotOptions>,

    sys_info: Arc<MI>,
}

impl ResourceBasedTuner<RealSysInfo> {
    /// Create an instance attempting to target the provided memory and cpu thresholds as values
    /// between 0 and 1.
    pub fn new(target_mem_usage: f64, target_cpu_usage: f64) -> Self {
        let opts = ResourceBasedSlotsOptions::builder()
            .target_mem_usage(target_mem_usage)
            .target_cpu_usage(target_cpu_usage)
            .build();
        let controller = ResourceController::new_with_sysinfo(
            opts,
            Arc::new(RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL)),
        );
        Self::new_from_controller(controller)
    }

    /// Create an instance using the fully configurable set of PID controller options
    pub fn new_from_options(options: ResourceBasedSlotsOptions) -> Self {
        let controller = ResourceController::new_with_sysinfo(
            options,
            Arc::new(RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL)),
        );
        Self::new_from_controller(controller)
    }
}

impl<MI> ResourceBasedTuner<MI> {
    fn new_from_controller(controller: ResourceController<MI>) -> Self {
        Self::new_from_shared_controller(Arc::new(controller))
    }

    pub(crate) fn new_from_shared_controller(slots: Arc<ResourceController<MI>>) -> Self {
        let sys_info = slots.sys_info_supplier.clone();
        Self {
            slots,
            wf_opts: None,
            act_opts: None,
            la_opts: None,
            nexus_opts: None,
            sys_info,
        }
    }

    /// Set workflow slot options
    pub fn with_workflow_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.wf_opts = Some(opts);
        self
    }

    /// Set activity slot options
    pub fn with_activity_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.act_opts = Some(opts);
        self
    }

    /// Set local activity slot options
    pub fn with_local_activity_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.la_opts = Some(opts);
        self
    }

    /// Set nexus slot options
    pub fn with_nexus_slots_options(&mut self, opts: ResourceSlotOptions) -> &mut Self {
        self.nexus_opts = Some(opts);
        self
    }

    /// Get sys info
    pub fn sys_info(&self) -> Arc<MI> {
        self.sys_info.clone()
    }
}

const DEFAULT_WF_SLOT_OPTS: ResourceSlotOptions = ResourceSlotOptions {
    min_slots: 2,
    max_slots: 10_000,
    ramp_throttle: Duration::from_millis(0),
};
const DEFAULT_ACT_SLOT_OPTS: ResourceSlotOptions = ResourceSlotOptions {
    min_slots: 1,
    max_slots: 10_000,
    ramp_throttle: Duration::from_millis(50),
};
const DEFAULT_NEXUS_SLOT_OPTS: ResourceSlotOptions = ResourceSlotOptions {
    min_slots: 1,
    max_slots: 10_000,
    // No ramp is chosen under the assumption that nexus tasks are unlikely to use many resources
    // and would prefer lowest latency over protection against oversubscription.
    ramp_throttle: Duration::from_millis(0),
};

/// Options for a specific slot type
#[derive(Debug, Clone, Copy, derive_more::Constructor)]
pub struct ResourceSlotOptions {
    /// Amount of slots of this type that will be issued regardless of any other checks
    min_slots: usize,
    /// Maximum amount of slots of this type permitted
    max_slots: usize,
    /// Minimum time we will wait (after passing the minimum slots number) between handing out new
    /// slots
    ramp_throttle: Duration,
}

/// Coordinates resource-based slot decisions using a shared resource sampler and PID controllers.
pub struct ResourceController<MI = RealSysInfo> {
    options: ResourceBasedSlotsOptions,
    sys_info_supplier: Arc<MI>,
    metrics: OnceLock<JoinHandle<()>>,
    pids: Mutex<PidControllers>,
    last_metric_vals: Arc<AtomicCell<LastMetricVals>>,
}
/// Implements [SlotSupplier] and attempts to maintain certain levels of resource usage when under
/// load.
///
/// It does so by using two PID controllers, one for memory and one for CPU, which are fed the
/// current usage levels of their respective resource as measurements. The user specifies a target
/// threshold for each, and slots are handed out if the output of both PID controllers is above some
/// defined threshold. See [ResourceBasedSlotsOptions] for the default PID controller settings.
pub(crate) struct ResourceBasedSlotsForType<MI, SK> {
    inner: Arc<ResourceController<MI>>,

    opts: ResourceSlotOptions,

    last_slot_issued_tx: watch::Sender<Instant>,
    last_slot_issued_rx: watch::Receiver<Instant>,

    // Only used for workflow slots - count of issued non-sticky slots
    issued_nonsticky: AtomicUsize,
    // Only used for workflow slots - count of issued sticky slots
    issued_sticky: AtomicUsize,

    _slot_kind: PhantomData<SK>,
}
/// Allows for the full customization of the PID options for a resource based tuner
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
pub struct ResourceBasedSlotsOptions {
    /// A value in the range [0.0, 1.0] representing the target memory usage.
    pub target_mem_usage: f64,
    /// A value in the range [0.0, 1.0] representing the target CPU usage.
    pub target_cpu_usage: f64,

    /// See [pid::Pid::p]
    #[builder(default = 5.0)]
    pub mem_p_gain: f64,
    /// See [pid::Pid::i]
    #[builder(default = 0.0)]
    pub mem_i_gain: f64,
    /// See [pid::Pid::d]
    #[builder(default = 1.0)]
    pub mem_d_gain: f64,
    /// If the mem PID controller outputs a value higher than this, we say the mem half of things
    /// will allow a slot
    #[builder(default = 0.25)]
    pub mem_output_threshold: f64,
    /// See [pid::Pid::d]
    #[builder(default = 5.0)]
    pub cpu_p_gain: f64,
    /// See [pid::Pid::i]
    #[builder(default = 0.0)]
    pub cpu_i_gain: f64,
    /// See [pid::Pid::d]
    #[builder(default = 1.0)]
    pub cpu_d_gain: f64,
    /// If the CPU PID controller outputs a value higher than this, we say the CPU half of things
    /// will allow a slot
    #[builder(default = 0.05)]
    pub cpu_output_threshold: f64,
}
struct PidControllers {
    mem: pid::Pid<f64>,
    cpu: pid::Pid<f64>,
}
struct MetricInstruments {
    attribs: MetricAttributes,
    mem_usage: GaugeF64,
    cpu_usage: GaugeF64,
    mem_pid_output: GaugeF64,
    cpu_pid_output: GaugeF64,
}
#[derive(Clone, Copy, Default)]
struct LastMetricVals {
    mem_output: f64,
    cpu_output: f64,
    mem_used_percent: f64,
    cpu_used_percent: f64,
}

impl PidControllers {
    fn new(options: &ResourceBasedSlotsOptions) -> Self {
        let mut mem = pid::Pid::new(options.target_mem_usage, 100.0);
        mem.p(options.mem_p_gain, 100)
            .i(options.mem_i_gain, 100)
            .d(options.mem_d_gain, 100);
        let mut cpu = pid::Pid::new(options.target_cpu_usage, 100.0);
        cpu.p(options.cpu_p_gain, 100)
            .i(options.cpu_i_gain, 100)
            .d(options.cpu_d_gain, 100);
        Self { mem, cpu }
    }
}

impl MetricInstruments {
    fn new(meter: TemporalMeter) -> Self {
        let mem_usage = meter.gauge_f64("resource_slots_mem_usage".into());
        let cpu_usage = meter.gauge_f64("resource_slots_cpu_usage".into());
        let mem_pid_output = meter.gauge_f64("resource_slots_mem_pid_output".into());
        let cpu_pid_output = meter.gauge_f64("resource_slots_cpu_pid_output".into());
        let attribs = meter.get_default_attributes().clone();
        Self {
            attribs,
            mem_usage,
            cpu_usage,
            mem_pid_output,
            cpu_pid_output,
        }
    }
}

/// Implementors provide information about system resource usage
pub trait SystemResourceInfo {
    /// Return total available system memory in bytes
    fn total_mem(&self) -> u64;
    /// Return memory used by the system in bytes
    fn used_mem(&self) -> u64;
    /// Return system used CPU as a float in the range [0.0, 1.0] where 1.0 is defined as all
    /// cores pegged
    fn used_cpu_percent(&self) -> f64;
    /// Return system used memory as a float in the range [0.0, 1.0]
    fn used_mem_percent(&self) -> f64 {
        self.used_mem() as f64 / self.total_mem() as f64
    }
}

#[async_trait::async_trait]
impl<MI, SK> SlotSupplier for ResourceBasedSlotsForType<MI, SK>
where
    MI: SystemResourceInfo + Send + Sync + 'static,
    SK: SlotKind + Send + Sync,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        if let Some(m) = ctx.get_metrics_meter() {
            self.inner.attach_metrics(m);
        }
        loop {
            if let Some(value) = self.issue_if_below_minimums(ctx) {
                return value;
            }
            let must_wait_for = self
                .opts
                .ramp_throttle
                .saturating_sub(self.time_since_last_issued());
            if must_wait_for > Duration::from_millis(0) {
                tokio::time::sleep(must_wait_for).await;
            }
            if let Some(p) = self.try_reserve_slot(ctx) {
                return p;
            } else {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        if let Some(m) = ctx.get_metrics_meter() {
            self.inner.attach_metrics(m);
        }
        if let v @ Some(_) = self.issue_if_below_minimums(ctx) {
            return v;
        }
        if self.time_since_last_issued() > self.opts.ramp_throttle
            && ctx.num_issued_slots() < self.opts.max_slots
            && self.inner.pid_decision()
            && self.inner.can_reserve()
        {
            Some(self.issue_slot(ctx))
        } else {
            None
        }
    }

    fn mark_slot_used(&self, _ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        // Really could use specialization here
        if let Some(SlotInfo::Workflow(info)) = ctx.info().map(|i| i.downcast()) {
            if info.is_sticky {
                self.issued_sticky.fetch_sub(1, Ordering::Relaxed);
            } else {
                self.issued_nonsticky.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    fn slot_supplier_kind(&self) -> String {
        "ResourceBased".to_string()
    }
}

impl<MI, SK> ResourceBasedSlotsForType<MI, SK>
where
    MI: SystemResourceInfo + Send + Sync,
    SK: SlotKind + Send + Sync,
{
    fn new(inner: Arc<ResourceController<MI>>, opts: ResourceSlotOptions) -> Self {
        let (tx, rx) = watch::channel(Instant::now());
        Self {
            opts,
            last_slot_issued_tx: tx,
            last_slot_issued_rx: rx,
            inner,
            issued_nonsticky: Default::default(),
            issued_sticky: Default::default(),
            _slot_kind: PhantomData,
        }
    }

    // Always be willing to hand out at least 1 slot for sticky and 1 for non-sticky to
    // avoid getting stuck.
    fn issue_if_below_minimums(
        &self,
        ctx: &dyn SlotReservationContext,
    ) -> Option<SlotSupplierPermit> {
        if ctx.num_issued_slots() < self.opts.min_slots {
            return Some(self.issue_slot(ctx));
        }
        if SK::kind() == SlotKindType::Workflow
            && (ctx.is_sticky() && self.issued_sticky.load(Ordering::Relaxed) == 0
                || !ctx.is_sticky() && self.issued_nonsticky.load(Ordering::Relaxed) == 0)
        {
            return Some(self.issue_slot(ctx));
        }

        None
    }

    fn issue_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        // Always be willing to hand out at least 1 slot for sticky and 1 for non-sticky to avoid
        // getting stuck.
        if SK::kind() == SlotKindType::Workflow {
            if ctx.is_sticky() {
                self.issued_sticky.fetch_add(1, Ordering::Relaxed);
            } else {
                self.issued_nonsticky.fetch_add(1, Ordering::Relaxed);
            }
        }
        let _ = self.last_slot_issued_tx.send(Instant::now());
        SlotSupplierPermit::default()
    }

    fn time_since_last_issued(&self) -> Duration {
        Instant::now()
            .checked_duration_since(*self.last_slot_issued_rx.borrow())
            .unwrap_or_default()
    }
}

impl<MI: SystemResourceInfo + Sync + Send + 'static> WorkerTuner for ResourceBasedTuner<MI> {
    fn workflow_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = WorkflowSlotKind> + Send + Sync> {
        let o = self.wf_opts.unwrap_or(DEFAULT_WF_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn activity_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = ActivitySlotKind> + Send + Sync> {
        let o = self.act_opts.unwrap_or(DEFAULT_ACT_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn local_activity_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = LocalActivitySlotKind> + Send + Sync> {
        let o = self.la_opts.unwrap_or(DEFAULT_ACT_SLOT_OPTS);
        self.slots.as_kind(o)
    }

    fn nexus_task_slot_supplier(
        &self,
    ) -> Arc<dyn SlotSupplier<SlotKind = NexusSlotKind> + Send + Sync> {
        let o = self.nexus_opts.unwrap_or(DEFAULT_NEXUS_SLOT_OPTS);
        self.slots.as_kind(o)
    }
}

impl<MI> std::fmt::Debug for ResourceController<MI> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceController").finish_non_exhaustive()
    }
}

impl ResourceController<RealSysInfo> {
    /// Create a resource controller using the default system resource sampler.
    pub fn new(options: ResourceBasedSlotsOptions) -> Self {
        Self::new_with_sysinfo(
            options,
            Arc::new(RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL)),
        )
    }
}

impl<MI: SystemResourceInfo + Sync + Send> ResourceController<MI> {
    /// Create a [ResourceBasedSlotsForType] for this instance which is willing to hand out
    /// `minimum` slots with no checks at all and `max` slots ever. Otherwise the underlying
    /// mem/cpu targets will attempt to be matched while under load.
    ///
    /// `ramp_throttle` determines how long this will pause for between making determinations about
    /// whether it is OK to hand out new slot(s). This is important to set to nonzero in situations
    /// where activities might use a lot of resources, because otherwise the implementation may
    /// hand out many slots quickly before resource usage has a chance to be reflected, possibly
    /// resulting in OOM (for example).
    pub(crate) fn as_kind<SK: SlotKind + Send + Sync>(
        self: &Arc<Self>,
        opts: ResourceSlotOptions,
    ) -> Arc<ResourceBasedSlotsForType<MI, SK>> {
        Arc::new(ResourceBasedSlotsForType::new(self.clone(), opts))
    }

    fn new_with_sysinfo(options: ResourceBasedSlotsOptions, sys_info: Arc<MI>) -> Self {
        Self {
            pids: Mutex::new(PidControllers::new(&options)),
            options,
            metrics: OnceLock::new(),
            sys_info_supplier: sys_info,
            last_metric_vals: Arc::new(AtomicCell::new(Default::default())),
        }
    }

    fn can_reserve(&self) -> bool {
        self.sys_info_supplier.used_mem_percent() <= self.options.target_mem_usage
    }

    /// Returns true if the pid controllers think a new slot should be given out
    fn pid_decision(&self) -> bool {
        let mut pids = self.pids.lock();
        let mem_used_percent = self.sys_info_supplier.used_mem_percent();
        let cpu_used_percent = self.sys_info_supplier.used_cpu_percent();
        let mem_output = pids.mem.next_control_output(mem_used_percent).output;
        let cpu_output = pids.cpu.next_control_output(cpu_used_percent).output;
        self.last_metric_vals.store(LastMetricVals {
            mem_output,
            cpu_output,
            mem_used_percent,
            cpu_used_percent,
        });
        mem_output > self.options.mem_output_threshold
            && cpu_output > self.options.cpu_output_threshold
    }

    fn attach_metrics(&self, metrics: TemporalMeter) {
        // Launch a task to periodically emit metrics
        self.metrics.get_or_init(move || {
            let m = MetricInstruments::new(metrics);
            let last_vals = self.last_metric_vals.clone();
            tokio::task::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    let lv = last_vals.load();
                    m.mem_pid_output.record(lv.mem_output, &m.attribs);
                    m.cpu_pid_output.record(lv.cpu_output, &m.attribs);
                    m.mem_usage.record(lv.mem_used_percent * 100., &m.attribs);
                    m.cpu_usage.record(lv.cpu_used_percent * 100., &m.attribs);
                    interval.tick().await;
                }
            })
        });
    }
}

/// Implements [SystemResourceInfo] using the [sysinfo] crate
#[derive(Debug)]
struct RealSysInfoInner {
    sys: Mutex<sysinfo::System>,
    total_mem: AtomicU64,
    cur_mem_usage: AtomicU64,
    cur_cpu_usage: AtomicU64,
    cgroup_cpu_info: CGroupCpuInfo<CgroupV2CpuFileSystem>,
}

impl RealSysInfoInner {
    fn refresh(&self) {
        let mut lock = self.sys.lock();
        lock.refresh_memory();
        if let Some(cgroup_limits) = lock.cgroup_limits() {
            self.total_mem
                .store(cgroup_limits.total_memory, Ordering::Release);
            // Gate on anonymous memory (`rss`) rather than `memory.current`
            // (`total - free`), which includes reclaimable page cache and can
            // permanently starve slot admission.
            self.cur_mem_usage
                .store(cgroup_limits.rss, Ordering::Release);

            let cpu = self.cgroup_cpu_info.calc_cpu_percent().unwrap_or_else(|| {
                // There won't be a cgroup cpu usage if there is no limit applied to the cgroup
                // or if an error is encountered when reading cpu.stat or cpu.max.
                // In these cases, fallback to global cpu usage.
                lock.refresh_cpu_usage();
                lock.global_cpu_usage() as f64 / 100.
            });
            self.cur_cpu_usage.store(cpu.to_bits(), Ordering::Release);
        } else {
            // Always update the total_mem b/c we could transiently fall to host if
            // lock.cgroup_limits() returns None.
            self.total_mem.store(lock.total_memory(), Ordering::Release);
            self.cur_mem_usage
                .store(lock.used_memory(), Ordering::Release);

            lock.refresh_cpu_usage();
            let cpu = lock.global_cpu_usage() as f64 / 100.;
            self.cur_cpu_usage.store(cpu.to_bits(), Ordering::Release);
        }
    }
}

/// Tracks host resource usage by refreshing metrics on a background thread.
pub struct RealSysInfo {
    inner: Arc<RealSysInfoInner>,
    shutdown_tx: mpsc::Sender<()>,
    shutdown_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RealSysInfo {
    /// Lower bound for the refresh interval.
    pub(crate) const MIN_REFRESH_INTERVAL: Duration = Duration::from_millis(100);
    /// Upper bound for the refresh interval.
    pub(crate) const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

    fn clamp_refresh_interval(interval: Duration) -> Duration {
        interval.clamp(Self::MIN_REFRESH_INTERVAL, Self::MAX_REFRESH_INTERVAL)
    }

    /// Spawns a background thread refreshing host metrics every `refresh_interval` (clamped to
    /// `[MIN_REFRESH_INTERVAL, MAX_REFRESH_INTERVAL]`).
    pub(crate) fn new(refresh_interval: Duration) -> Self {
        let refresh_interval = Self::clamp_refresh_interval(refresh_interval);
        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let total_mem = sys.total_memory();
        let inner = Arc::new(RealSysInfoInner {
            sys: Mutex::new(sys),
            cur_mem_usage: AtomicU64::new(0),
            cur_cpu_usage: AtomicU64::new(0),
            total_mem: AtomicU64::new(total_mem),
            cgroup_cpu_info: CGroupCpuInfo::new(CgroupV2CpuFileSystem),
        });
        inner.refresh();

        let thread_clone = inner.clone();
        let (tx, rx) = mpsc::channel::<()>();
        let handle = thread::Builder::new()
            .name("temporal-real-sysinfo".to_string())
            .spawn(move || {
                loop {
                    thread_clone.refresh();
                    let r = rx.recv_timeout(refresh_interval);
                    if matches!(r, Err(mpsc::RecvTimeoutError::Disconnected)) || r.is_ok() {
                        return;
                    }
                }
            })
            .expect("failed to spawn RealSysInfo refresh thread");

        Self {
            inner,
            shutdown_tx: tx,
            shutdown_handle: Mutex::new(Some(handle)),
        }
    }
}

impl SystemResourceInfo for RealSysInfo {
    fn total_mem(&self) -> u64 {
        self.inner.total_mem.load(Ordering::Acquire)
    }

    fn used_mem(&self) -> u64 {
        self.inner.cur_mem_usage.load(Ordering::Acquire)
    }

    fn used_cpu_percent(&self) -> f64 {
        f64::from_bits(self.inner.cur_cpu_usage.load(Ordering::Acquire))
    }
}

impl Drop for RealSysInfo {
    fn drop(&mut self) {
        let _res = self.shutdown_tx.send(());
        if let Some(handle) = self.shutdown_handle.lock().take() {
            let _ = handle.join();
        }
    }
}

/// Parsed representation of the CPU quota and slice period exposed by the
/// cgroup `cpu.max` control file.
#[derive(Debug)]
struct CGroupCpuLimits {
    quota: CpuQuota,
    period: u64,
}

/// Enumerates whether a cgroup enforces a specific CPU quota or allows
/// unlimited usage.
#[derive(Debug)]
enum CpuQuota {
    Unlimited,
    Limited(u64),
}

trait CGroupCpuFileSystem {
    fn read_cpu_stat_file(&self) -> Option<String>;
    fn read_cpu_limit_file(&self) -> Option<String>;
}

/// Tracks recent cgroup CPU usage statistics while abstracting the backing
/// filesystem access for ease of testing.
#[derive(Debug)]
struct CGroupCpuInfo<T: CGroupCpuFileSystem> {
    prev_cpu_usage: AtomicU64,
    last_refresh: AtomicCell<Option<Instant>>,
    fs: T,
}

impl<T: CGroupCpuFileSystem> CGroupCpuInfo<T> {
    /// Creates a new tracker and immediately primes it by reading the current
    /// CPU usage so future percentage calculations have baseline data.
    fn new(fs: T) -> Self {
        let s = Self {
            last_refresh: AtomicCell::new(None),
            prev_cpu_usage: AtomicU64::new(0),
            fs,
        };
        s.calc_cpu_percent();
        s
    }

    /// Reads the current CPU usage and limits for the cgroup and calculates the usage percentage based on the
    /// limits and the elapsed time from the last calculation. Returns `None` until enough data has been collected
    /// to determine a delta or when quotas are unlimited.
    fn calc_cpu_percent(&self) -> Option<f64> {
        let usage = self.read_cpu_usage()?;
        let limits = self.read_cpus_limit()?;

        let previous_usage = self.prev_cpu_usage.swap(usage, Ordering::AcqRel);
        let now = Instant::now();
        let last_updated = self.last_refresh.swap(Some(now));

        if previous_usage > 0
            && let Some(last_updated) = last_updated
            && let CpuQuota::Limited(quota) = limits.quota
            && quota > 0
        {
            let elapsed_us = (now - last_updated).as_micros() as f64;
            let usage_delta = usage.saturating_sub(previous_usage) as f64;

            let cpu_percent = usage_delta * limits.period as f64 / (quota as f64 * elapsed_us);
            Some(cpu_percent)
        } else {
            None
        }
    }

    /// Reads the usage_usec value from the cpu.stat file as a u64.
    /// Returns None if the file cannot be read or if the value cannot be parsed into u64.
    ///
    /// The cpu.stat file is expected to be a multiline file where each line is `key value`
    fn read_cpu_usage(&self) -> Option<u64> {
        let stat = self.fs.read_cpu_stat_file()?;
        stat.lines().find_map(|line| {
            let mut parts = line.split_whitespace();
            match (parts.next(), parts.next()) {
                (Some("usage_usec"), Some(value)) => value.parse::<u64>().ok(),
                _ => None,
            }
        })
    }

    /// Reads the cpu quota and period from the cpu.max file.
    /// Returns None if the file cannot be read, or the quota/limit cannot be parsed
    ///
    /// The cpu.max file is expected to be in the format 'quota period'
    fn read_cpus_limit(&self) -> Option<CGroupCpuLimits> {
        let limit_text = self.fs.read_cpu_limit_file()?;
        let mut parts = limit_text.split_whitespace();
        let quota_str = parts.next()?;
        let period_str = parts.next()?;

        let quota = if quota_str == "max" {
            CpuQuota::Unlimited
        } else {
            CpuQuota::Limited(quota_str.parse().ok()?)
        };
        let period = period_str.parse().ok()?;

        Some(CGroupCpuLimits { quota, period })
    }
}

/// Implementation of the `CGroupCpuFileSystem` that reads directly
/// from the host cgroup v2 hierarchy.
#[derive(Debug)]
struct CgroupV2CpuFileSystem;

impl CgroupV2CpuFileSystem {
    const BASE_PATH: &'static str = "/sys/fs/cgroup";
}

impl CGroupCpuFileSystem for CgroupV2CpuFileSystem {
    fn read_cpu_stat_file(&self) -> Option<String> {
        let path = PathBuf::from(Self::BASE_PATH).join("cpu.stat");
        fs::read_to_string(path).ok()
    }

    fn read_cpu_limit_file(&self) -> Option<String> {
        let path = PathBuf::from(Self::BASE_PATH).join("cpu.max");
        fs::read_to_string(path).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abstractions::MeteredPermitDealer, telemetry::metrics::MetricsContext,
        worker::WorkflowSlotKind,
    };
    use std::{
        cell::RefCell,
        env,
        hint::black_box,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        thread::sleep,
    };

    struct FakeMIS {
        used: Arc<AtomicU64>,
    }
    impl FakeMIS {
        fn new() -> (Arc<Self>, Arc<AtomicU64>) {
            let used = Arc::new(AtomicU64::new(0));
            (Arc::new(Self { used: used.clone() }), used)
        }
    }
    impl SystemResourceInfo for FakeMIS {
        fn total_mem(&self) -> u64 {
            100_000
        }

        fn used_mem(&self) -> u64 {
            self.used.load(Ordering::Acquire)
        }

        fn used_cpu_percent(&self) -> f64 {
            0.0
        }
    }

    fn test_options() -> ResourceBasedSlotsOptions {
        ResourceBasedSlotsOptions::builder()
            .target_mem_usage(0.8)
            .target_cpu_usage(1.0)
            .build()
    }

    #[test]
    fn refresh_interval_clamped() {
        // In-range passes through; out-of-range clamps at both ends.
        assert_eq!(
            RealSysInfo::clamp_refresh_interval(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
        assert_eq!(
            RealSysInfo::clamp_refresh_interval(Duration::from_millis(1)),
            RealSysInfo::MIN_REFRESH_INTERVAL
        );
        assert_eq!(
            RealSysInfo::clamp_refresh_interval(Duration::from_secs(3600)),
            RealSysInfo::MAX_REFRESH_INTERVAL
        );
    }

    #[test]
    fn realsysinfo_reports_sane_values() {
        let sys_info = RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL);
        assert!(sys_info.total_mem() > 0);
        let cpu = sys_info.used_cpu_percent();
        assert!((0.0..=1.0).contains(&cpu), "cpu {cpu} out of range");
    }

    #[test]
    fn mem_workflow_sync() {
        let (fmis, used) = FakeMIS::new();
        let rbs = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(
            rbs.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        let pd_s = pd.clone().into_sticky();
        // Start with too high usage
        used.store(90_000, Ordering::Release);
        // Show workflow will always allow 1 each of sticky/non-sticky
        assert!(rbs.try_reserve_slot(&pd).is_some());
        assert!(rbs.try_reserve_slot(&pd_s).is_some());
        assert!(rbs.try_reserve_slot(&pd).is_none());
        assert!(rbs.try_reserve_slot(&pd_s).is_none());
        used.store(0, Ordering::Release);
        // Now it's willing to hand out slots again when usage is zero
        assert!(rbs.try_reserve_slot(&pd).is_some());
        assert!(rbs.try_reserve_slot(&pd_s).is_some());
    }

    #[test]
    fn mem_activity_sync() {
        let (fmis, used) = FakeMIS::new();
        let rbs = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis))
            .as_kind::<ActivitySlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(
            rbs.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        // Start with too high usage
        used.store(90_000, Ordering::Release);
        assert!(rbs.try_reserve_slot(&pd).is_none());
        used.store(0, Ordering::Release);
        // Now it's willing to hand out slots again when usage is zero
        assert!(rbs.try_reserve_slot(&pd).is_some());
    }

    #[test]
    fn shared_controller_has_same_behavior_across_tuners() {
        let (fmis, used) = FakeMIS::new();
        let controller = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis));
        let slot_options = ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        };
        let mut first_tuner = ResourceBasedTuner::new_from_shared_controller(controller.clone());
        first_tuner.with_activity_slots_options(slot_options);
        let mut second_tuner = ResourceBasedTuner::new_from_shared_controller(controller);
        second_tuner.with_activity_slots_options(slot_options);
        let first_supplier = first_tuner.activity_task_slot_supplier();
        let second_supplier = second_tuner.activity_task_slot_supplier();
        let first_context = MeteredPermitDealer::new(
            first_supplier.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        let second_context = MeteredPermitDealer::new(
            second_supplier.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );

        used.store(90_000, Ordering::Release);
        assert!(first_supplier.try_reserve_slot(&first_context).is_none());
        assert!(second_supplier.try_reserve_slot(&second_context).is_none());

        used.store(0, Ordering::Release);
        assert!(first_supplier.try_reserve_slot(&first_context).is_some());
        assert!(second_supplier.try_reserve_slot(&second_context).is_some());
    }

    #[tokio::test]
    async fn mem_workflow_async() {
        let (fmis, used) = FakeMIS::new();
        used.store(90_000, Ordering::Release);
        let rbs = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(
            rbs.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        let pd_s = pd.clone().into_sticky();
        let order = crossbeam_queue::ArrayQueue::new(2);
        // Show workflow will always allow 1 each of sticky/non-sticky
        let _p1 = rbs.reserve_slot(&pd).await;
        let _p2 = rbs.reserve_slot(&pd_s).await;
        // Now we need to have some memory get freed before the next call resolves
        let waits_free = async {
            rbs.reserve_slot(&pd).await;
            order.push(2).unwrap();
        };
        let frees = async {
            used.store(70_000, Ordering::Release);
            order.push(1).unwrap();
        };
        tokio::join!(waits_free, frees);
        assert_eq!(order.pop(), Some(1));
        assert_eq!(order.pop(), Some(2));
    }

    #[tokio::test]
    async fn mem_activity_async() {
        let (fmis, used) = FakeMIS::new();
        used.store(90_000, Ordering::Release);
        let rbs = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis))
            .as_kind::<ActivitySlotKind>(ResourceSlotOptions {
            min_slots: 0,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(
            rbs.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        let order = crossbeam_queue::ArrayQueue::new(2);
        let waits_free = async {
            rbs.reserve_slot(&pd).await;
            order.push(2).unwrap();
        };
        let frees = async {
            used.store(70_000, Ordering::Release);
            order.push(1).unwrap();
        };
        tokio::join!(waits_free, frees);
        assert_eq!(order.pop(), Some(1));
        assert_eq!(order.pop(), Some(2));
    }

    #[test]
    fn minimum_respected() {
        let (fmis, used) = FakeMIS::new();
        let rbs = Arc::new(ResourceController::new_with_sysinfo(test_options(), fmis))
            .as_kind::<WorkflowSlotKind>(ResourceSlotOptions {
            min_slots: 2,
            max_slots: 100,
            ramp_throttle: Duration::from_millis(0),
        });
        let pd = MeteredPermitDealer::new(
            rbs.clone(),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        );
        used.store(90_000, Ordering::Release);
        let _p1 = pd.try_acquire_owned().unwrap();
        let _p2 = pd.try_acquire_owned().unwrap();
        assert!(pd.try_acquire_owned().is_err());
    }

    #[derive(Clone)]
    struct FakeCGroupFS {
        stat: Rc<RefCell<Option<String>>>,
        limit: Rc<RefCell<Option<String>>>,
    }

    impl FakeCGroupFS {
        fn new(stat: Rc<RefCell<Option<String>>>, limit: Rc<RefCell<Option<String>>>) -> Self {
            Self { stat, limit }
        }
    }

    impl CGroupCpuFileSystem for FakeCGroupFS {
        fn read_cpu_stat_file(&self) -> Option<String> {
            self.stat.borrow().clone()
        }

        fn read_cpu_limit_file(&self) -> Option<String> {
            self.limit.borrow().clone()
        }
    }

    #[test]
    fn cgroup_quota_respected() {
        let stat = Rc::new(RefCell::new(Some("usage_usec 500".into())));
        let limit = Rc::new(RefCell::new(Some("1000 1000".into())));
        let fake_fs = FakeCGroupFS::new(stat.clone(), limit.clone());

        let cgroup_info = CGroupCpuInfo::new(fake_fs);

        std::thread::sleep(Duration::from_micros(1_000_u64));

        // No additional usage -> percentage should resolve to exactly zero.
        assert_eq!(Some(0.0), cgroup_info.calc_cpu_percent());

        // Add some usage such that it we should be above 0, but < 1
        stat.replace(Some("usage_usec 1000".into()));
        std::thread::sleep(Duration::from_micros(1_000_u64));

        let cpu_percent = cgroup_info
            .calc_cpu_percent()
            .expect("expected usage value");
        assert!(
            cpu_percent > 0. && cpu_percent < 1.0,
            "epected cpu usage between (0.0, 1.0)"
        );

        // Simulate a large usage increase that should saturate the quota.
        stat.replace(Some("usage_usec 1000000".into()));

        let cpu_percent = cgroup_info
            .calc_cpu_percent()
            .expect("expected usage value");
        assert!(
            cpu_percent > 1.0,
            "expected cpu usage greater than 1.0, got {cpu_percent}"
        );
    }

    #[test]
    fn cgroup_unlimited_quota_is_ignored() {
        let stat = Rc::new(RefCell::new(Some("usage_usec 1000".into())));
        let limit = Rc::new(RefCell::new(Some("max 1000".into())));

        let cgroup_info = CGroupCpuInfo::new(FakeCGroupFS::new(stat.clone(), limit.clone()));

        std::thread::sleep(Duration::from_micros(1_000_u64));

        // Unlimited quota is treated as disabled regardless of usage.
        assert!(cgroup_info.calc_cpu_percent().is_none());
        stat.replace(Some("usage_usec 2000".into()));

        std::thread::sleep(Duration::from_micros(1_000_u64));
        assert!(cgroup_info.calc_cpu_percent().is_none());
    }

    #[test]
    fn cgroup_stat_file_temporarily_unavailable() {
        let stat = Rc::new(RefCell::new(None));
        let limit = Rc::new(RefCell::new(Some("1000 1000".into())));

        let cgroup_info = CGroupCpuInfo::new(FakeCGroupFS::new(stat.clone(), limit.clone()));

        std::thread::sleep(Duration::from_micros(1_000_u64));

        assert!(cgroup_info.calc_cpu_percent().is_none());

        // When available, the next call finishes initialization
        stat.replace(Some("usage_usec 1000".into()));
        std::thread::sleep(Duration::from_micros(1_000_u64));
        assert!(cgroup_info.calc_cpu_percent().is_none());

        // A third call properly calculates percentage
        stat.replace(Some("usage_usec 1500".into()));
        std::thread::sleep(Duration::from_micros(1_000_u64));
        let cpu_percent = cgroup_info
            .calc_cpu_percent()
            .expect("expected usage value");
        assert!(cpu_percent > 0., "expected cpu percent > 0");
    }

    #[test]
    fn cgroup_realsysinfo_uses_cgroup_limits_cpu() {
        if env::var("CGROUP_TESTS_ENABLED").is_err() {
            eprintln!("Skipping: cgroup tests not enabled");
            return;
        }

        let sys_info = RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL);
        let cgroup_info = CGroupCpuInfo::new(CgroupV2CpuFileSystem);
        let CGroupCpuLimits { quota, period } = cgroup_info
            .read_cpus_limit()
            .expect("unable to read cpu quota.");

        let CpuQuota::Limited(limit) = quota else {
            eprintln!("Skipping: cpu quota reported unlimited");
            return;
        };

        // Prime the internal CPU tracker with some activity.
        let baseline_time = Instant::now();
        let prev_usage = cgroup_info
            .read_cpu_usage()
            .expect("unable to read cpu usage baseline");

        // Consume some CPU so that the next refresh observes a measurable delta.
        burn_cpu(Duration::from_millis(300));

        let cpu_usage = sys_info.used_cpu_percent();
        let measurement_end = Instant::now();
        let current_usage = cgroup_info
            .read_cpu_usage()
            .expect("unable to read cpu usage after refresh");

        let elapsed_us = measurement_end.duration_since(baseline_time).as_micros() as f64;
        let usage_delta = current_usage.saturating_sub(prev_usage) as f64;
        assert!(usage_delta > 0.0, "zero cpu usage delta observed");

        let expected_cpu = usage_delta * period as f64 / (limit as f64 * elapsed_us);
        let cpu_diff = (cpu_usage - expected_cpu).abs();
        const CPU_TOLERANCE: f64 = 0.3;
        assert!(
            cpu_diff <= CPU_TOLERANCE,
            "RealSysInfo CPU percent {cpu_usage:.3} diverged from cgroup calculation \
         {expected_cpu:.3} by {cpu_diff:.3}"
        );
    }

    #[test]
    fn cgroup_realsysinfo_uses_cgroup_limits_mem() {
        if env::var("CGROUP_TESTS_ENABLED").is_err() {
            eprintln!("Skipping: cgroup tests not enabled");
            return;
        }

        let mut sys = sysinfo::System::new();
        sys.refresh_memory();
        let current_mem = sys
            .cgroup_limits()
            .expect("unable to read cgroup memory details");

        if current_mem.total_memory == sys.total_memory() {
            eprintln!("Skipping: no memory limit detected. limit == total host memory");
            return;
        }

        let sys_info = RealSysInfo::new(RealSysInfo::MIN_REFRESH_INTERVAL);

        assert_eq!(
            sys_info.total_mem(),
            current_mem.total_memory, // sysinfo::System does min(cgroup limit, host total) internally
            "RealSysInfo total_mem should equal min(cgroup limit, host total)"
        );

        // Usage is gated on anonymous memory, not `memory.current`, so page
        // cache doesn't count against the limit.
        let cur_used = current_mem.rss;
        let half_total = current_mem.total_memory / 2;
        let expected_percentage = if cur_used > half_total {
            cur_used as f64 / current_mem.total_memory as f64
        } else {
            0.5
        };
        // allocate to half of memory
        let to_allocate: usize = (half_total).saturating_sub(cur_used) as usize;
        let _buf = black_box(vec![1u8; to_allocate]);

        // make sure we sleep enough to let real_sys_info need a refresh
        sleep(Duration::from_millis(200));

        let percentage = sys_info.used_mem_percent();
        let diff = (percentage - expected_percentage).abs();

        assert!(
            diff < 0.2,
            "RealSysInfo used_mem_percentage should be ~= {}% but was {}",
            expected_percentage * 100.0,
            percentage * 100.0
        );
    }

    fn burn_cpu(duration: Duration) {
        let start = Instant::now();
        let mut value: u64 = 0;
        while start.elapsed() < duration {
            value = value.wrapping_add(1);
            black_box(value);
        }
    }
}
