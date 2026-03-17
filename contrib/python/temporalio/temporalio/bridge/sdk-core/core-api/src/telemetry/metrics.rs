use crate::dbg_panic;
use std::{
    any::Any,
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    ops::Deref,
    sync::{Arc, OnceLock},
    time::Duration,
};

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
    fn counter(&self, params: MetricParameters) -> Counter;
    fn histogram(&self, params: MetricParameters) -> Histogram;
    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64;
    /// Create a histogram which records Durations. Implementations should choose to emit in
    /// either milliseconds or seconds depending on how they have been configured.
    /// [MetricParameters::unit] should be overwritten by implementations to be `ms` or `s`
    /// accordingly.
    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration;
    fn gauge(&self, params: MetricParameters) -> Gauge;
    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64;
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct MetricParameters {
    /// The name for the new metric/instrument
    #[builder(setter(into))]
    pub name: Cow<'static, str>,
    /// A description that will appear in metadata if the backend supports it
    #[builder(setter(into), default = "\"\".into()")]
    pub description: Cow<'static, str>,
    /// Unit information that will appear in metadata if the backend supports it
    #[builder(setter(into), default = "\"\".into()")]
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
#[derive(derive_more::Constructor, Clone, Debug)]
pub struct TemporalMeter {
    pub inner: Arc<dyn CoreMeter>,
    pub default_attribs: NewAttributes,
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
}

/// Attributes which are provided every time a call to record a specific metric is made.
/// Implementors must be very cheap to clone, as these attributes will be re-used frequently.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MetricAttributes {
    #[cfg(feature = "otel_impls")]
    OTel {
        kvs: Arc<Vec<opentelemetry::KeyValue>>,
    },
    Prometheus {
        labels: Arc<OrderedPromLabelSet>,
    },
    Buffer(BufferAttributes),
    Dynamic(Arc<dyn CustomMetricAttributes>),
    Empty,
}

/// A reference to some attributes created lang side.
pub trait CustomMetricAttributes: Debug + Send + Sync {
    /// Must be implemented to work around existing type system restrictions, see
    /// [here](https://internals.rust-lang.org/t/downcast-not-from-any-but-from-any-trait/16736/12)
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// Options that are attached to metrics on a per-call basis
#[derive(Clone, Debug, Default, derive_more::Constructor)]
pub struct NewAttributes {
    pub attributes: Vec<MetricKeyValue>,
}
impl NewAttributes {
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

/// A K/V pair that can be used to label a specific recording of a metric
#[derive(Clone, Debug, PartialEq)]
pub struct MetricKeyValue {
    pub key: String,
    pub value: MetricValue,
}
impl MetricKeyValue {
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
    String(String),
    Int(i64),
    Float(f64),
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

#[derive(Clone)]
pub struct LazyBoundMetric<T, B> {
    metric: T,
    attributes: MetricAttributes,
    bound_cache: OnceLock<B>,
}
impl<T, B> LazyBoundMetric<T, B> {
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.attributes = new_attributes;
        self.bound_cache = OnceLock::new();
    }
}

pub trait CounterBase: Send + Sync {
    fn adds(&self, value: u64);
}
pub type Counter = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>,
    Arc<dyn CounterBase>,
>;
impl Counter {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    pub fn add(&self, value: u64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.adds(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl CounterBase for Counter {
    fn adds(&self, value: u64) {
        // TODO: Replace all of these with below when stable
        //   https://doc.rust-lang.org/std/sync/struct.OnceLock.html#method.get_or_try_init
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn CounterBase>
                })
        });
        bound.adds(value);
    }
}
impl MetricAttributable<Counter> for Counter {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Counter, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

pub trait HistogramBase: Send + Sync {
    fn records(&self, value: u64);
}
pub type Histogram = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>,
    Arc<dyn HistogramBase>,
>;
impl Histogram {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
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

pub trait HistogramF64Base: Send + Sync {
    fn records(&self, value: f64);
}
pub type HistogramF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    Arc<dyn HistogramF64Base>,
>;
impl HistogramF64 {
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    ) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
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

pub trait HistogramDurationBase: Send + Sync {
    fn records(&self, value: Duration);
}
pub type HistogramDuration = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    Arc<dyn HistogramDurationBase>,
>;
impl HistogramDuration {
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    ) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    pub fn record(&self, value: Duration, attributes: &MetricAttributes) {
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
impl HistogramDurationBase for HistogramDuration {
    fn records(&self, value: Duration) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramDurationBase>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<HistogramDuration> for HistogramDuration {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<HistogramDuration, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

pub trait GaugeBase: Send + Sync {
    fn records(&self, value: u64);
}
pub type Gauge = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>,
    Arc<dyn GaugeBase>,
>;
impl Gauge {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
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
impl GaugeBase for Gauge {
    fn records(&self, value: u64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn GaugeBase>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<Gauge> for Gauge {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Gauge, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

pub trait GaugeF64Base: Send + Sync {
    fn records(&self, value: f64);
}
pub type GaugeF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>,
    Arc<dyn GaugeF64Base>,
>;
impl GaugeF64 {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
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

#[derive(Debug, Clone)]
pub enum MetricEvent<I: BufferInstrumentRef> {
    Create {
        params: MetricParameters,
        /// Once you receive this event, call `set` on this with the initialized instrument
        /// reference
        populate_into: LazyBufferInstrument<I>,
        kind: MetricKind,
    },
    CreateAttributes {
        /// Once you receive this event, call `set` on this with the initialized attributes
        populate_into: BufferAttributes,
        /// If not `None`, use these already-initialized attributes as the base (extended with
        /// `attributes`) for the ones you are about to initialize.
        append_from: Option<BufferAttributes>,
        attributes: Vec<MetricKeyValue>,
    },
    Update {
        instrument: LazyBufferInstrument<I>,
        attributes: BufferAttributes,
        update: MetricUpdateVal,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    GaugeF64,
    Histogram,
    HistogramF64,
    HistogramDuration,
}
#[derive(Debug, Clone, Copy)]
pub enum MetricUpdateVal {
    Delta(u64),
    DeltaF64(f64),
    Value(u64),
    ValueF64(f64),
    Duration(Duration),
}

pub trait MetricCallBufferer<I: BufferInstrumentRef>: Send + Sync {
    fn retrieve(&self) -> Vec<MetricEvent<I>>;
}

/// A lazy reference to some metrics buffer attributes
pub type BufferAttributes = LazyRef<Arc<dyn CustomMetricAttributes + 'static>>;

/// Types lang uses to contain references to its lang-side defined instrument references must
/// implement this marker trait
pub trait BufferInstrumentRef {}
/// A lazy reference to a metrics buffer instrument
pub type LazyBufferInstrument<T> = LazyRef<Arc<T>>;

#[derive(Debug, Clone)]
pub struct LazyRef<T> {
    to_be_initted: Arc<OnceLock<T>>,
}
impl<T> LazyRef<T> {
    pub fn hole() -> Self {
        Self {
            to_be_initted: Arc::new(OnceLock::new()),
        }
    }

    /// Get the reference you previously initialized
    ///
    /// # Panics
    /// If `set` has not already been called. You must set the reference before using it.
    pub fn get(&self) -> &T {
        self.to_be_initted
            .get()
            .expect("You must initialize the reference before using it")
    }

    /// Assigns a value to fill this reference.
    /// Returns according to semantics of [OnceLock].
    pub fn set(&self, val: T) -> Result<(), T> {
        self.to_be_initted.set(val)
    }
}

#[derive(Debug)]
pub struct NoOpCoreMeter;
impl CoreMeter for NoOpCoreMeter {
    fn new_attributes(&self, _: NewAttributes) -> MetricAttributes {
        MetricAttributes::Dynamic(Arc::new(NoOpAttributes))
    }

    fn extend_attributes(&self, existing: MetricAttributes, _: NewAttributes) -> MetricAttributes {
        existing
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

pub struct NoOpInstrument;
macro_rules! impl_no_op {
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

#[derive(Debug, Clone)]
pub struct NoOpAttributes;
impl CustomMetricAttributes for NoOpAttributes {
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self as Arc<dyn Any + Send + Sync>
    }
}

#[cfg(feature = "otel_impls")]
mod otel_impls {
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
}

/// Maintains a mapping of metric labels->values with a defined ordering, used for Prometheus labels
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderedPromLabelSet {
    attributes: BTreeMap<String, MetricValue>,
}

impl OrderedPromLabelSet {
    pub const fn new() -> Self {
        Self {
            attributes: BTreeMap::new(),
        }
    }
    pub fn keys_ordered(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(|s| s.as_str())
    }
    pub fn as_prom_labels(&self) -> HashMap<&str, String> {
        let mut labels = HashMap::new();
        for (k, v) in self.attributes.iter() {
            labels.insert(k.as_str(), v.to_string());
        }
        labels
    }
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
