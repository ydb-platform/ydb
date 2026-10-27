use crate::KeyValue;
use core::fmt;
use std::sync::Arc;

#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::BoundSyncInstrument;
use super::SyncInstrument;

/// An instrument that records increasing values.
///
/// [`Counter`] can be cloned to create multiple handles to the same instrument. If a [`Counter`] needs to be shared,
/// users are recommended to clone the [`Counter`] instead of creating duplicate [`Counter`]s for the same metric. Creating
/// duplicate [`Counter`]s for the same metric could lower SDK performance.
#[derive(Clone)]
#[non_exhaustive]
pub struct Counter<T>(Arc<dyn SyncInstrument<T> + Send + Sync>);

impl<T> fmt::Debug for Counter<T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("Counter<{}>", std::any::type_name::<T>()))
    }
}

impl<T> Counter<T> {
    /// Create a new counter.
    pub fn new(inner: Arc<dyn SyncInstrument<T> + Send + Sync>) -> Self {
        Counter(inner)
    }

    /// Records an increment to the counter.
    pub fn add(&self, value: T, attributes: &[KeyValue]) {
        self.0.measure(value, attributes)
    }

    /// Binds this counter to a fixed set of attributes.
    #[cfg(feature = "experimental_metrics_bound_instruments")]
    pub fn bind(&self, attributes: &[KeyValue]) -> BoundCounter<T> {
        BoundCounter(Arc::from(self.0.bind(attributes)))
    }
}

/// An async instrument that records increasing values.
#[derive(Clone)]
#[non_exhaustive]
pub struct ObservableCounter<T> {
    _marker: std::marker::PhantomData<T>,
}

impl<T> ObservableCounter<T> {
    /// Create a new observable counter.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        ObservableCounter {
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T> fmt::Debug for ObservableCounter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "ObservableCounter<{}>",
            std::any::type_name::<T>()
        ))
    }
}

/// A counter bound to a fixed set of attributes.
///
/// Created by calling [`Counter::bind`] with an attribute set. All subsequent
/// [`add`](BoundCounter::add) calls use the pre-resolved attributes, bypassing
/// per-call attribute lookup for significantly better performance.
///
/// `BoundCounter` can be cloned cheaply to share a single bound state across
/// threads or modules without re-binding. The underlying tracker is reclaimed
/// when the last clone is dropped.
#[cfg(feature = "experimental_metrics_bound_instruments")]
#[derive(Clone)]
#[must_use = "dropping a BoundCounter immediately is a no-op; store it to benefit from pre-bound attributes"]
pub struct BoundCounter<T>(Arc<dyn BoundSyncInstrument<T> + Send + Sync>);

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T> fmt::Debug for BoundCounter<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("BoundCounter<{}>", std::any::type_name::<T>()))
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T> BoundCounter<T> {
    /// Records an increment to the counter using the pre-bound attributes.
    pub fn add(&self, value: T) {
        self.0.measure(value)
    }
}
