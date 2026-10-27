//! # No-op OpenTelemetry Metrics Implementation
//!
//! This implementation is returned as the global Meter if no `MeterProvider`
//! has been set. It is expected to have minimal resource utilization and
//! runtime impact.
use crate::{
    metrics::{InstrumentProvider, Meter, MeterProvider},
    otel_debug, KeyValue,
};
use std::sync::Arc;

#[cfg(feature = "experimental_metrics_bound_instruments")]
use super::instruments::BoundSyncInstrument;
use super::instruments::SyncInstrument;

/// A no-op instance of a `MetricProvider`
#[derive(Debug, Default)]
pub struct NoopMeterProvider {
    _private: (),
}

impl NoopMeterProvider {
    /// Create a new no-op meter provider.
    pub fn new() -> Self {
        NoopMeterProvider { _private: () }
    }
}

impl MeterProvider for NoopMeterProvider {
    fn meter_with_scope(&self, scope: crate::InstrumentationScope) -> Meter {
        otel_debug!(name: "NoopMeterProvider.MeterCreation", meter_name = scope.name(), message = "Meter was obtained from a NoopMeterProvider. No metrics will be recorded. If global::meter_with_scope()/meter() was used, ensure that a valid MeterProvider is set globally before creating Meter.");
        Meter::new(Arc::new(NoopMeter::new()))
    }
}

/// A no-op instance of a `Meter`
#[derive(Debug, Default)]
pub(crate) struct NoopMeter {
    _private: (),
}

impl NoopMeter {
    /// Create a new no-op meter core.
    pub(crate) fn new() -> Self {
        NoopMeter { _private: () }
    }
}

impl InstrumentProvider for NoopMeter {}

/// A no-op sync instrument
#[derive(Debug, Default)]
pub(crate) struct NoopSyncInstrument {
    _private: (),
}

impl NoopSyncInstrument {
    /// Create a new no-op sync instrument
    pub(crate) fn new() -> Self {
        NoopSyncInstrument { _private: () }
    }
}

impl<T: Send + Sync + 'static> SyncInstrument<T> for NoopSyncInstrument {
    fn measure(&self, _value: T, _attributes: &[KeyValue]) {
        // Ignored
    }

    #[cfg(feature = "experimental_metrics_bound_instruments")]
    fn bind(&self, _attributes: &[KeyValue]) -> Box<dyn BoundSyncInstrument<T> + Send + Sync> {
        Box::new(NoopBoundSyncInstrument::new())
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
pub(crate) struct NoopBoundSyncInstrument {
    _private: (),
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl NoopBoundSyncInstrument {
    pub(crate) fn new() -> Self {
        NoopBoundSyncInstrument { _private: () }
    }
}

#[cfg(feature = "experimental_metrics_bound_instruments")]
impl<T> BoundSyncInstrument<T> for NoopBoundSyncInstrument {
    fn measure(&self, _measurement: T) {
        // Ignored
    }
}
