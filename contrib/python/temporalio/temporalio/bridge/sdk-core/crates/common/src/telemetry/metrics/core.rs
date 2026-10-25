//! Metrics related code only needed by core and core-based SDKs

use crate::telemetry::metrics::{MetricKeyValue, MetricParameters};
use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, OnceLock},
    time::Duration,
};

/// A reference to some attributes created lang side.
pub trait CustomMetricAttributes: Debug + Send + Sync {
    /// Must be implemented to work around existing type system restrictions, see
    /// [here](https://internals.rust-lang.org/t/downcast-not-from-any-but-from-any-trait/16736/12)
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

/// Events produced by the metrics buffering layer for deferred processing by lang.
#[derive(Debug, Clone)]
pub enum MetricEvent<I: BufferInstrumentRef> {
    /// Request to create a new metric instrument.
    Create {
        /// Parameters (name, description, unit) for the new instrument.
        params: MetricParameters,
        /// Once you receive this event, call `set` on this with the initialized instrument
        /// reference
        populate_into: LazyBufferInstrument<I>,
        /// The kind of metric instrument to create.
        kind: MetricKind,
    },
    /// Request to create a new set of metric attributes.
    CreateAttributes {
        /// Once you receive this event, call `set` on this with the initialized attributes
        populate_into: BufferAttributes,
        /// If not `None`, use these already-initialized attributes as the base (extended with
        /// `attributes`) for the ones you are about to initialize.
        append_from: Option<BufferAttributes>,
        /// The key-value pairs for the new attribute set.
        attributes: Vec<MetricKeyValue>,
    },
    /// A metric recording to apply to an already-created instrument.
    Update {
        /// The instrument to record against.
        instrument: LazyBufferInstrument<I>,
        /// The attributes to associate with this recording.
        attributes: BufferAttributes,
        /// The value to record.
        update: MetricUpdateVal,
    },
}
/// The kind of metric instrument to create.
#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    /// A monotonically increasing `u64` counter.
    Counter,
    /// A `u64` gauge that can go up or down.
    Gauge,
    /// An `f64` gauge that can go up or down.
    GaugeF64,
    /// A `u64` histogram for recording distributions.
    Histogram,
    /// An `f64` histogram for recording distributions.
    HistogramF64,
    /// A histogram that records [`Duration`] values.
    HistogramDuration,
    /// An `i64` counter that can go up or down.
    UpDownCounter,
}
/// The value to record when updating a metric instrument.
#[derive(Debug, Clone, Copy)]
pub enum MetricUpdateVal {
    /// A `u64` delta increment (for counters/histograms).
    Delta(u64),
    /// An `f64` delta increment (for f64 histograms).
    DeltaF64(f64),
    /// An absolute `u64` value (for gauges).
    Value(u64),
    /// An absolute `f64` value (for f64 gauges).
    ValueF64(f64),
    /// A duration observation (for duration histograms).
    Duration(Duration),
    /// A signed `i64` delta (for up-down counters).
    SignedDelta(i64),
}

/// Collects buffered metric events for later replay into a real metrics backend.
pub trait MetricCallBufferer<I: BufferInstrumentRef>: Send + Sync {
    /// Drain and return all buffered metric events.
    fn retrieve(&self) -> Vec<MetricEvent<I>>;
}

/// A lazy reference to some metrics buffer attributes
pub type BufferAttributes = LazyRef<Arc<dyn CustomMetricAttributes + 'static>>;

/// Types lang uses to contain references to its lang-side defined instrument references must
/// implement this marker trait
pub trait BufferInstrumentRef {}
/// A lazy reference to a metrics buffer instrument
pub type LazyBufferInstrument<T> = LazyRef<Arc<T>>;

/// A shared, lazily-initialized reference. Created as an empty "hole" and filled later via [`set`](Self::set).
#[derive(Debug, Clone)]
pub struct LazyRef<T> {
    to_be_initted: Arc<OnceLock<T>>,
}
impl<T> LazyRef<T> {
    /// Create an uninitialized hole that must be filled with [`set`](Self::set) before use.
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
