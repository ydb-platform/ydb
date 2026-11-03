use crate::{dbg_panic, telemetry::TaskQueueLabelStrategy};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    ops::Deref,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "core-telemetry-bridge")]
pub mod core;

/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME: &str = "workflow_endtoend_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "workflow_task_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_replay_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_execution_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "activity_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME: &str = "activity_execution_latency";

/// Helps define buckets once in terms of millis, but also generates a seconds version
macro_rules! define_latency_buckets {
    ($(($metric_name:pat, $name:ident, $sec_name:ident, [$($bucket:expr),*])),*) => {
        $(
            pub(super) static $name: &[f64] = &[$($bucket,)*];
            pub(super) static $sec_name: &[f64] = &[$( $bucket / 1000.0, )*];
        )*

        /// Returns the default histogram buckets that lang should use for a given metric name if
        /// they have not been overridden by the user. If `use_seconds` is true, returns buckets
        /// in terms of seconds rather than milliseconds.
        ///
        /// The name must *not* be prefixed with `temporal_`
        pub fn default_buckets_for(histo_name: &str, use_seconds: bool) -> &'static [f64] {
            match histo_name {
                $(
                    $metric_name => { if use_seconds { &$sec_name } else { &$name } },
                )*
            }
        }
    };
}

define_latency_buckets!(
    (
        WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
        WF_LATENCY_MS_BUCKETS,
        WF_LATENCY_S_BUCKETS,
        [
            100.,
            500.,
            1000.,
            1500.,
            2000.,
            5000.,
            10_000.,
            30_000.,
            60_000.,
            120_000.,
            300_000.,
            600_000.,
            1_800_000.,  // 30 min
            3_600_000.,  //  1 hr
            30_600_000., // 10 hrs
            8.64e7       // 24 hrs
        ]
    ),
    (
        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME
            | WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WF_TASK_MS_BUCKETS,
        WF_TASK_S_BUCKETS,
        [1., 10., 20., 50., 100., 200., 500., 1000.]
    ),
    (
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME,
        ACT_EXE_MS_BUCKETS,
        ACT_EXE_S_BUCKETS,
        [50., 100., 500., 1000., 5000., 10_000., 60_000.]
    ),
    (
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME
            | ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        TASK_SCHED_TO_START_MS_BUCKETS,
        TASK_SCHED_TO_START_S_BUCKETS,
        [100., 500., 1000., 5000., 10_000., 100_000., 1_000_000.]
    ),
    (
        _,
        DEFAULT_MS_BUCKETS,
        DEFAULT_S_BUCKETS,
        [50., 100., 500., 1000., 2500., 10_000.]
    )
);

/// Implementors of this trait are expected to be defined in each language's bridge.
/// The implementor is responsible for the allocation/instantiation of new metric meters which
/// Core has requested.
pub trait CoreMeter: Send + Sync + Debug {
    /// Given some k/v pairs, create a return a new instantiated instance of metric attributes.
    /// Only [MetricAttributes] created by this meter can be used when calling record on instruments
    /// created by this meter.
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes;
    /// Extend some existing attributes with new values. Implementations should create new instances
    /// when doing so, rather than mutating whatever is backing the passed in `existing` attributes.
    /// Ideally that new instance retains a ref to the extended old attribute, promoting re-use.
    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes;
    /// Create a new counter instrument.
    fn counter(&self, params: MetricParameters) -> Counter;

    /// Create a counter with in-memory tracking for worker heartbeating reporting
    fn counter_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_counter: HeartbeatMetricType,
    ) -> Counter {
        let primary_counter = self.counter(params);

        Counter::new_with_in_memory(primary_counter.primary.metric.clone(), in_memory_counter)
    }

    /// Create a new histogram instrument recording `u64` values.
    fn histogram(&self, params: MetricParameters) -> Histogram;
    /// Create a new histogram instrument recording `f64` values.
    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64;
    /// Create a histogram which records Durations. Implementations should choose to emit in
    /// either milliseconds or seconds depending on how they have been configured.
    /// [MetricParameters::unit] should be overwritten by implementations to be `ms` or `s`
    /// accordingly.
    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration;

    /// Create a histogram duration with in-memory tracking for worker heartbeating reporting
    fn histogram_duration_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_hist: HeartbeatMetricType,
    ) -> HistogramDuration {
        let primary_hist = self.histogram_duration(params);

        HistogramDuration::new_with_in_memory(primary_hist.primary.metric.clone(), in_memory_hist)
    }
    /// Create a new gauge instrument recording `u64` values.
    fn gauge(&self, params: MetricParameters) -> Gauge;

    /// Create a gauge with in-memory tracking for worker heartbeating reporting
    fn gauge_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_metrics: HeartbeatMetricType,
    ) -> Gauge {
        let primary_gauge = self.gauge(params.clone());
        Gauge::new_with_in_memory(primary_gauge.primary.metric.clone(), in_memory_metrics)
    }

    /// Create a new gauge instrument recording `f64` values.
    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64;

    /// Create a new up-down counter instrument recording `i64` values.
    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter;
}

/// Provides a generic way to record metrics in memory.
/// This can be done either with individual metrics or more fine-grained metrics
/// that vary by a set of labels for the same metric.
#[derive(Clone, Debug)]
pub enum HeartbeatMetricType {
    /// A single counter shared across all label values.
    Individual(Arc<AtomicU64>),
    /// Per-label-value counters, keyed by a specific label.
    WithLabel {
        /// The label key to match against metric attributes.
        label_key: String,
        /// Map from label value to its atomic counter.
        metrics: HashMap<String, Arc<AtomicU64>>,
    },
}

impl HeartbeatMetricType {
    fn record_counter(&self, delta: u64) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.fetch_add(delta, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { .. } => {
                dbg_panic!("Counter does not support in-memory metric with labels");
            }
        }
    }

    fn record_histogram_observation(&self) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.fetch_add(1, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { .. } => {
                dbg_panic!("Histogram does not support in-memory metric with labels");
            }
        }
    }

    fn record_gauge(&self, value: u64, attributes: &MetricAttributes) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.store(value, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { label_key, metrics } => {
                if let Some(metric) = label_value_from_attributes(attributes, label_key.as_str())
                    .and_then(|label_value| metrics.get(label_value.as_str()))
                {
                    metric.store(value, Ordering::Relaxed)
                }
            }
        }
    }
}

fn label_value_from_attributes(attributes: &MetricAttributes, key: &str) -> Option<String> {
    match attributes {
        MetricAttributes::Prometheus { labels } => labels.as_prom_labels().get(key).cloned(),
        #[cfg(feature = "otel")]
        MetricAttributes::OTel { kvs } => kvs
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string()),
        MetricAttributes::NoOp(labels) => labels.get(key).cloned(),
        _ => None,
    }
}

/// Parameters used when creating a new metric instrument (name, description, unit).
#[derive(Debug, Clone, bon::Builder)]
pub struct MetricParameters {
    /// The name for the new metric/instrument
    #[builder(into)]
    pub name: Cow<'static, str>,
    /// A description that will appear in metadata if the backend supports it
    #[builder(into, default = Cow::Borrowed(""))]
    pub description: Cow<'static, str>,
    /// Unit information that will appear in metadata if the backend supports it
    #[builder(into, default = Cow::Borrowed(""))]
    pub unit: Cow<'static, str>,
}
impl From<&'static str> for MetricParameters {
    fn from(value: &'static str) -> Self {
        Self {
            name: value.into(),
            description: Default::default(),
            unit: Default::default(),
        }
    }
}

/// Wraps a [CoreMeter] to enable the attaching of default labels to metrics. Cloning is cheap.
#[derive(Clone, Debug)]
pub struct TemporalMeter {
    inner: Arc<dyn CoreMeter>,
    default_attribs: MetricAttributes,
    task_queue_label_strategy: TaskQueueLabelStrategy,
}

impl TemporalMeter {
    /// Create a new TemporalMeter.
    pub fn new(
        meter: Arc<dyn CoreMeter>,
        default_attribs: NewAttributes,
        task_queue_label_strategy: TaskQueueLabelStrategy,
    ) -> Self {
        Self {
            default_attribs: meter.new_attributes(default_attribs),
            inner: meter,
            task_queue_label_strategy,
        }
    }

    /// Creates a TemporalMeter that records nothing
    pub fn no_op() -> Self {
        Self {
            inner: Arc::new(NoOpCoreMeter),
            default_attribs: MetricAttributes::NoOp(Arc::new(Default::default())),
            task_queue_label_strategy: TaskQueueLabelStrategy::UseNormal,
        }
    }

    /// Returns the default attributes this meter uses.
    pub fn get_default_attributes(&self) -> &MetricAttributes {
        &self.default_attribs
    }

    /// Returns the Task Queue labeling strategy this meter uses.
    pub fn get_task_queue_label_strategy(&self) -> &TaskQueueLabelStrategy {
        &self.task_queue_label_strategy
    }

    /// Add some new attributes to the default set already in this meter.
    pub fn merge_attributes(&mut self, new_attribs: NewAttributes) {
        self.default_attribs = self.extend_attributes(self.default_attribs.clone(), new_attribs);
    }
}

impl Deref for TemporalMeter {
    type Target = dyn CoreMeter;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl CoreMeter for Arc<dyn CoreMeter> {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.as_ref().new_attributes(attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        self.as_ref().extend_attributes(existing, attribs)
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        self.as_ref().counter(params)
    }
    fn histogram(&self, params: MetricParameters) -> Histogram {
        self.as_ref().histogram(params)
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        self.as_ref().histogram_f64(params)
    }

    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration {
        self.as_ref().histogram_duration(params)
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        self.as_ref().gauge(params)
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        self.as_ref().gauge_f64(params)
    }

    fn up_down_counter(&self, params: MetricParameters) -> UpDownCounter {
        self.as_ref().up_down_counter(params)
    }
}

/// Attributes which are provided every time a call to record a specific metric is made.
/// Implementors must be very cheap to clone, as these attributes will be re-used frequently.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MetricAttributes {
    /// OpenTelemetry-backed attributes with key-value pairs.
    #[cfg(feature = "otel")]
    OTel {
        /// The OpenTelemetry key-value pairs for this attribute set.
        kvs: Arc<Vec<opentelemetry::KeyValue>>,
    },
    /// Prometheus-backed attributes with ordered labels.
    Prometheus {
        /// The ordered label set.
        labels: Arc<OrderedPromLabelSet>,
    },
    /// Buffered attributes used by core-based SDKs for deferred metric initialization.
    #[cfg(feature = "core-telemetry-bridge")]
    Buffer(core::BufferAttributes),
    /// Dynamic attributes backed by a lang-side custom implementation.
    #[cfg(feature = "core-telemetry-bridge")]
    Dynamic(Arc<dyn core::CustomMetricAttributes>),
    /// No-op attributes that store labels but do not record.
    NoOp(Arc<HashMap<String, String>>),
    /// Empty placeholder attributes.
    Empty,
}

/// Options that are attached to metrics on a per-call basis
#[derive(Clone, Debug, Default, derive_more::Constructor)]
pub struct NewAttributes {
    /// The key-value pairs for this attribute set.
    pub attributes: Vec<MetricKeyValue>,
}
impl NewAttributes {
    /// Append additional key-value pairs to this attribute set.
    pub fn extend(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        self.attributes.extend(new_kvs)
    }
}
impl<I> From<I> for NewAttributes
where
    I: IntoIterator<Item = MetricKeyValue>,
{
    fn from(value: I) -> Self {
        Self {
            attributes: value.into_iter().collect(),
        }
    }
}

impl From<NewAttributes> for HashMap<String, String> {
    fn from(value: NewAttributes) -> Self {
        value
            .attributes
            .into_iter()
            .map(|kv| (kv.key, kv.value.to_string()))
            .collect()
    }
}

/// A K/V pair that can be used to label a specific recording of a metric
#[derive(Clone, Debug, PartialEq)]
pub struct MetricKeyValue {
    /// The label key.
    pub key: String,
    /// The label value.
    pub value: MetricValue,
}
impl MetricKeyValue {
    /// Create a new key-value pair.
    pub fn new(key: impl Into<String>, value: impl Into<MetricValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Values metric labels may assume
#[derive(Clone, Debug, PartialEq, derive_more::From)]
pub enum MetricValue {
    /// A string label value.
    String(String),
    /// An integer label value.
    Int(i64),
    /// A floating-point label value.
    Float(f64),
    /// A boolean label value.
    Bool(bool),
    // can add array if needed
}
impl From<&'static str> for MetricValue {
    fn from(value: &'static str) -> Self {
        MetricValue::String(value.to_string())
    }
}
impl Display for MetricValue {
    fn fmt(&self, f1: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricValue::String(s) => write!(f1, "{s}"),
            MetricValue::Int(i) => write!(f1, "{i}"),
            MetricValue::Float(f) => write!(f1, "{f}"),
            MetricValue::Bool(b) => write!(f1, "{b}"),
        }
    }
}

/// Trait for metric instruments that can be bound to a set of attributes.
pub trait MetricAttributable<Base> {
    /// Replace any existing attributes on this metric with new ones, and return a new copy
    /// of the metric, or a base version, which can be used to record values.
    ///
    /// Note that this operation is relatively expensive compared to simply recording a value
    /// without any additional attributes, so users should prefer to save the metric instance
    /// after calling this, and use the value-only methods afterward.
    ///
    /// This operation may fail if the underlying metrics implementation disallows the registration
    /// of a new metric, or encounters any other issue.
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Base, Box<dyn std::error::Error>>;
}

/// A metric that lazily binds to its attributes on first use, caching the result.
#[derive(Clone)]
pub struct LazyBoundMetric<T, B> {
    metric: T,
    attributes: MetricAttributes,
    bound_cache: OnceLock<B>,
}
impl<T, B> LazyBoundMetric<T, B> {
    /// Replace the attributes and invalidate the cached binding.
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.attributes = new_attributes;
        self.bound_cache = OnceLock::new();
    }
}

/// Base trait for counter implementations.
pub trait CounterBase: Send + Sync {
    /// Increment the counter by `value`.
    fn adds(&self, value: u64);
}

/// The lazy-bound inner type for [`Counter`].
pub type CounterImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>,
    Arc<dyn CounterBase>,
>;

/// A counter metric instrument that optionally tracks values in memory for heartbeating.
#[derive(Clone)]
pub struct Counter {
    primary: CounterImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl Counter {
    /// Create a new counter from an attributable metric source.
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }

    /// Create a new counter with an additional in-memory tracker for heartbeat reporting.
    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }

    /// Increment the counter by `value` with the given attributes.
    pub fn add(&self, value: u64, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => base.adds(value),
            Err(e) => {
                dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_counter(value);
        }
    }

    /// Replace the attributes on the primary metric.
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl CounterBase for Counter {
    fn adds(&self, value: u64) {
        // TODO: Replace all of these with below when stable
        //   https://doc.rust-lang.org/std/sync/struct.OnceLock.html#method.get_or_try_init
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn CounterBase>
                })
        });
        bound.adds(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_counter(value);
        }
    }
}
impl MetricAttributable<Counter> for Counter {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Counter, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(Counter {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

/// Base trait for `u64` histogram implementations.
pub trait HistogramBase: Send + Sync {
    /// Record a `u64` observation.
    fn records(&self, value: u64);
}
/// A `u64` histogram metric instrument.
pub type Histogram = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>,
    Arc<dyn HistogramBase>,
>;
impl Histogram {
    /// Create a new histogram from an attributable metric source.
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    /// Record a `u64` value with the given attributes.
    pub fn record(&self, value: u64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl HistogramBase for Histogram {
    fn records(&self, value: u64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramBase>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<Histogram> for Histogram {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Histogram, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

/// Base trait for `f64` histogram implementations.
pub trait HistogramF64Base: Send + Sync {
    /// Record an `f64` observation.
    fn records(&self, value: f64);
}
/// An `f64` histogram metric instrument.
pub type HistogramF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    Arc<dyn HistogramF64Base>,
>;
impl HistogramF64 {
    /// Create a new `f64` histogram from an attributable metric source.
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    ) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    /// Record an `f64` value with the given attributes.
    pub fn record(&self, value: f64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl HistogramF64Base for HistogramF64 {
    fn records(&self, value: f64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramF64Base>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<HistogramF64> for HistogramF64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<HistogramF64, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

/// Base trait for duration histogram implementations.
pub trait HistogramDurationBase: Send + Sync {
    /// Record a duration observation.
    fn records(&self, value: Duration);
}

/// The lazy-bound inner type for [`HistogramDuration`].
pub type HistogramDurationImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    Arc<dyn HistogramDurationBase>,
>;

/// A histogram that records [`Duration`] values, optionally tracking in memory for heartbeating.
#[derive(Clone)]
pub struct HistogramDuration {
    primary: HistogramDurationImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl HistogramDuration {
    /// Create a new duration histogram from an attributable metric source.
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }
    /// Create a new duration histogram with an additional in-memory tracker for heartbeat reporting.
    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }
    /// Record a duration value with the given attributes.
    pub fn record(&self, value: Duration, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_histogram_observation();
        }
    }

    /// Replace the attributes on the primary metric.
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl HistogramDurationBase for HistogramDuration {
    fn records(&self, value: Duration) {
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramDurationBase>
                })
        });
        bound.records(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_histogram_observation();
        }
    }
}
impl MetricAttributable<HistogramDuration> for HistogramDuration {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<HistogramDuration, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(HistogramDuration {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

/// Base trait for `u64` gauge implementations.
pub trait GaugeBase: Send + Sync {
    /// Record a `u64` gauge value.
    fn records(&self, value: u64);
}

/// The lazy-bound inner type for [`Gauge`].
pub type GaugeImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>,
    Arc<dyn GaugeBase>,
>;

/// A gauge metric instrument that optionally tracks values in memory for heartbeating.
#[derive(Clone)]
pub struct Gauge {
    primary: GaugeImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl Gauge {
    /// Create a new gauge from an attributable metric source.
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }

    /// Create a new gauge with an additional in-memory tracker for heartbeat reporting.
    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }

    /// Record a `u64` gauge value with the given attributes.
    pub fn record(&self, value: u64, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => base.records(value),
            Err(e) => {
                dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_gauge(value, attributes);
        }
    }

    /// Replace the attributes on the primary metric.
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl GaugeBase for Gauge {
    fn records(&self, value: u64) {
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn GaugeBase>
                })
        });
        bound.records(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_gauge(value, &self.primary.attributes);
        }
    }
}
impl MetricAttributable<Gauge> for Gauge {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Gauge, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(Gauge {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

/// Base trait for `f64` gauge implementations.
pub trait GaugeF64Base: Send + Sync {
    /// Record an `f64` gauge value.
    fn records(&self, value: f64);
}
/// An `f64` gauge metric instrument.
pub type GaugeF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>,
    Arc<dyn GaugeF64Base>,
>;
impl GaugeF64 {
    /// Create a new `f64` gauge from an attributable metric source.
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    /// Record an `f64` gauge value with the given attributes.
    pub fn record(&self, value: f64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl GaugeF64Base for GaugeF64 {
    fn records(&self, value: f64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn GaugeF64Base>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<GaugeF64> for GaugeF64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<GaugeF64, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

/// Base trait for up-down counter implementations.
pub trait UpDownCounterBase: Send + Sync {
    /// Add a signed value to the up-down counter.
    fn adds(&self, value: i64);
}

/// The lazy-bound inner type for [`UpDownCounter`].
pub type UpDownCounterImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn UpDownCounterBase>> + Send + Sync>,
    Arc<dyn UpDownCounterBase>,
>;

/// An up-down counter metric instrument that supports both positive and negative deltas.
#[derive(Clone)]
pub struct UpDownCounter {
    primary: UpDownCounterImpl,
}
impl UpDownCounter {
    /// Create a new up-down counter from an attributable metric source.
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn UpDownCounterBase>> + Send + Sync>,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
        }
    }

    /// Add a signed `i64` value with the given attributes.
    pub fn add(&self, value: i64, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => base.adds(value),
            Err(e) => {
                dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
            }
        }
    }

    /// Replace the attributes on the primary metric.
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl UpDownCounterBase for UpDownCounter {
    fn adds(&self, value: i64) {
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn UpDownCounterBase>
                })
        });
        bound.adds(value);
    }
}
impl MetricAttributable<UpDownCounter> for UpDownCounter {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<UpDownCounter, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };
        Ok(UpDownCounter { primary })
    }
}

/// A [`CoreMeter`] implementation that discards all metric recordings.
#[derive(Debug)]
pub struct NoOpCoreMeter;
impl CoreMeter for NoOpCoreMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::NoOp(Arc::new(attribs.into()))
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::NoOp(labels) = existing {
            let mut labels = (*labels).clone();
            labels.extend::<HashMap<String, String>>(attribs.into());
            MetricAttributes::NoOp(Arc::new(labels))
        } else {
            dbg_panic!("Must use NoOp attributes with a NoOp metric implementation");
            existing
        }
    }

    fn counter(&self, _: MetricParameters) -> Counter {
        Counter::new(Arc::new(NoOpInstrument))
    }

    fn histogram(&self, _: MetricParameters) -> Histogram {
        Histogram::new(Arc::new(NoOpInstrument))
    }

    fn histogram_f64(&self, _: MetricParameters) -> HistogramF64 {
        HistogramF64::new(Arc::new(NoOpInstrument))
    }

    fn histogram_duration(&self, _: MetricParameters) -> HistogramDuration {
        HistogramDuration::new(Arc::new(NoOpInstrument))
    }

    fn gauge(&self, _: MetricParameters) -> Gauge {
        Gauge::new(Arc::new(NoOpInstrument))
    }

    fn gauge_f64(&self, _: MetricParameters) -> GaugeF64 {
        GaugeF64::new(Arc::new(NoOpInstrument))
    }

    fn up_down_counter(&self, _: MetricParameters) -> UpDownCounter {
        UpDownCounter::new(Arc::new(NoOpInstrument))
    }
}

macro_rules! impl_metric_attributable {
    ($base_trait:ident, $rt:ty, $init:expr) => {
        impl MetricAttributable<Box<dyn $base_trait>> for $rt {
            fn with_attributes(
                &self,
                _: &MetricAttributes,
            ) -> Result<Box<dyn $base_trait>, Box<dyn std::error::Error>> {
                Ok(Box::new($init))
            }
        }
    };
}

/// A no-op metric instrument that discards all recordings.
pub struct NoOpInstrument;
macro_rules! impl_no_op {
    ($base_trait:ident, signed) => {
        impl_metric_attributable!($base_trait, NoOpInstrument, NoOpInstrument);
        impl $base_trait for NoOpInstrument {
            fn adds(&self, _: i64) {}
        }
    };
    ($base_trait:ident, $value_type:ty) => {
        impl_metric_attributable!($base_trait, NoOpInstrument, NoOpInstrument);
        impl $base_trait for NoOpInstrument {
            fn records(&self, _: $value_type) {}
        }
    };
    ($base_trait:ident) => {
        impl_metric_attributable!($base_trait, NoOpInstrument, NoOpInstrument);
        impl $base_trait for NoOpInstrument {
            fn adds(&self, _: u64) {}
        }
    };
}
impl_no_op!(CounterBase);
impl_no_op!(HistogramBase, u64);
impl_no_op!(HistogramF64Base, f64);
impl_no_op!(HistogramDurationBase, Duration);
impl_no_op!(GaugeBase, u64);
impl_no_op!(GaugeF64Base, f64);
impl_no_op!(UpDownCounterBase, signed);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    #[test]
    fn in_memory_attributes_provide_label_values() {
        let meter = NoOpCoreMeter;
        let base_attrs = meter.new_attributes(NewAttributes::default());
        let attrs = meter.extend_attributes(
            base_attrs,
            NewAttributes::from(vec![MetricKeyValue::new("poller_type", "workflow_task")]),
        );

        let value = Arc::new(AtomicU64::new(0));
        let mut metrics = HashMap::new();
        metrics.insert("workflow_task".to_string(), value.clone());
        let heartbeat_metric = HeartbeatMetricType::WithLabel {
            label_key: "poller_type".to_string(),
            metrics,
        };

        heartbeat_metric.record_gauge(3, &attrs);

        assert_eq!(value.load(Ordering::Relaxed), 3);
        assert_eq!(
            label_value_from_attributes(&attrs, "poller_type").as_deref(),
            Some("workflow_task")
        );
    }
}

#[cfg(feature = "otel")]
mod otel {
    use super::*;
    use opentelemetry::{KeyValue, metrics};

    #[derive(Clone)]
    struct InstrumentWithAttributes<I> {
        inner: I,
        attributes: MetricAttributes,
    }

    impl From<MetricKeyValue> for KeyValue {
        fn from(kv: MetricKeyValue) -> Self {
            KeyValue::new(kv.key, kv.value)
        }
    }

    impl From<MetricValue> for opentelemetry::Value {
        fn from(mv: MetricValue) -> Self {
            match mv {
                MetricValue::String(s) => opentelemetry::Value::String(s.into()),
                MetricValue::Int(i) => opentelemetry::Value::I64(i),
                MetricValue::Float(f) => opentelemetry::Value::F64(f),
                MetricValue::Bool(b) => opentelemetry::Value::Bool(b),
            }
        }
    }

    impl MetricAttributable<Box<dyn CounterBase>> for metrics::Counter<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn CounterBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl CounterBase for InstrumentWithAttributes<metrics::Counter<u64>> {
        fn adds(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.add(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn GaugeBase>> for metrics::Gauge<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn GaugeBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl GaugeBase for InstrumentWithAttributes<metrics::Gauge<u64>> {
        fn records(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn GaugeF64Base>> for metrics::Gauge<f64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn GaugeF64Base>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl GaugeF64Base for InstrumentWithAttributes<metrics::Gauge<f64>> {
        fn records(&self, value: f64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn HistogramBase>> for metrics::Histogram<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn HistogramBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl HistogramBase for InstrumentWithAttributes<metrics::Histogram<u64>> {
        fn records(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn HistogramF64Base>> for metrics::Histogram<f64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn HistogramF64Base>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl HistogramF64Base for InstrumentWithAttributes<metrics::Histogram<f64>> {
        fn records(&self, value: f64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn UpDownCounterBase>> for opentelemetry::metrics::UpDownCounter<i64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn UpDownCounterBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl UpDownCounterBase for InstrumentWithAttributes<opentelemetry::metrics::UpDownCounter<i64>> {
        fn adds(&self, value: i64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.add(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }
}

/// Maintains a mapping of metric labels->values with a defined ordering, used for Prometheus labels
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderedPromLabelSet {
    attributes: BTreeMap<String, MetricValue>,
}

impl OrderedPromLabelSet {
    /// Create an empty label set.
    pub const fn new() -> Self {
        Self {
            attributes: BTreeMap::new(),
        }
    }
    /// Iterate over label keys in sorted order.
    pub fn keys_ordered(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(|s| s.as_str())
    }
    /// Return a map of label keys to their string values, suitable for Prometheus.
    pub fn as_prom_labels(&self) -> HashMap<&str, String> {
        let mut labels = HashMap::new();
        for (k, v) in self.attributes.iter() {
            labels.insert(k.as_str(), v.to_string());
        }
        labels
    }
    /// Insert a key-value pair, replacing dashes with underscores per Prometheus conventions.
    pub fn add_kv(&mut self, kv: MetricKeyValue) {
        // Replace '-' with '_' per Prom naming requirements
        self.attributes.insert(kv.key.replace('-', "_"), kv.value);
    }
}

impl From<NewAttributes> for OrderedPromLabelSet {
    fn from(n: NewAttributes) -> Self {
        let mut me = Self::default();
        for kv in n.attributes {
            me.add_kv(kv);
        }
        me
    }
}

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PrefixedMetricsMeter<CM> {
    prefix: String,
    meter: CM,
}
impl<CM: CoreMeter> CoreMeter for PrefixedMetricsMeter<CM> {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.meter.new_attributes(attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        self.meter.extend_attributes(existing, attribs)
    }

    fn counter(&self, mut params: MetricParameters) -> Counter {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.counter(params)
    }

    fn histogram(&self, mut params: MetricParameters) -> Histogram {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram(params)
    }

    fn histogram_f64(&self, mut params: MetricParameters) -> HistogramF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_f64(params)
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_duration(params)
    }

    fn gauge(&self, mut params: MetricParameters) -> Gauge {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge(params)
    }

    fn gauge_f64(&self, mut params: MetricParameters) -> GaugeF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge_f64(params)
    }

    fn up_down_counter(&self, mut params: MetricParameters) -> UpDownCounter {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.up_down_counter(params)
    }
}
