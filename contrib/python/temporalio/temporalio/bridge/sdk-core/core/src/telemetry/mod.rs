//! This module helps with the initialization and management of telemetry. IE: Metrics and tracing.
//! Logs from core are all traces, which may be exported to the console, in memory, or externally.

mod log_export;
pub(crate) mod metrics;
#[cfg(feature = "otel")]
mod otel;
#[cfg(feature = "prom")]
mod prometheus_meter;
#[cfg(feature = "prom")]
mod prometheus_server;

// Always export bucket configuration function since it's used by both OTel and Prometheus
pub use metrics::default_buckets_for;

#[cfg(feature = "otel")]
pub use metrics::{
    ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME, ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
    MetricsCallBuffer, WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
    WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME, WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
    WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
};
#[cfg(feature = "otel")]
pub use otel::build_otlp_metric_exporter;
#[cfg(feature = "prom")]
pub use prometheus_server::start_prometheus_metric_exporter;

pub use log_export::{CoreLogBuffer, CoreLogBufferedConsumer, CoreLogStreamConsumer};

use crate::telemetry::{log_export::CoreLogConsumerLayer, metrics::PrefixedMetricsMeter};
use itertools::Itertools;
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::VecDeque,
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use temporal_sdk_core_api::telemetry::{
    CoreLog, CoreTelemetry, Logger, TelemetryOptions, TelemetryOptionsBuilder,
    metrics::{CoreMeter, MetricKeyValue, NewAttributes, TemporalMeter},
};
use tracing::{Level, Subscriber};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";

const FORWARD_LOG_BUFFER_SIZE: usize = 2048;

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_level`.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{other_level},temporal_sdk_core={core_level},temporal_client={core_level},temporal_sdk={core_level}"
    )
}

/// Holds initialized tracing/metrics exporters, etc
pub struct TelemetryInstance {
    metric_prefix: String,
    logs_out: Option<Mutex<CoreLogBuffer>>,
    metrics: Option<Arc<dyn CoreMeter + 'static>>,
    /// The tracing subscriber which is associated with this telemetry instance. May be `None` if
    /// the user has not opted into any tracing configuration.
    trace_subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,
    attach_service_name: bool,
}

impl TelemetryInstance {
    fn new(
        trace_subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,
        logs_out: Option<Mutex<CoreLogBuffer>>,
        metric_prefix: String,
        metrics: Option<Arc<dyn CoreMeter + 'static>>,
        attach_service_name: bool,
    ) -> Self {
        Self {
            metric_prefix,
            logs_out,
            metrics,
            trace_subscriber,
            attach_service_name,
        }
    }

    /// Return the trace subscriber associated with the telemetry options/instance. Can be used
    /// to manually set the default for a thread or globally using the `tracing` crate, or with
    /// [set_trace_subscriber_for_current_thread].
    pub fn trace_subscriber(&self) -> Option<Arc<dyn Subscriber + Send + Sync>> {
        self.trace_subscriber.clone()
    }

    /// Some metric meters cannot be initialized until after a tokio runtime has started and after
    /// other telemetry has initted (ex: prometheus). They can be attached here.
    pub fn attach_late_init_metrics(&mut self, meter: Arc<dyn CoreMeter + 'static>) {
        self.metrics = Some(meter);
    }

    /// Returns our wrapper for metric meters, including the `metric_prefix` from
    /// [TelemetryOptions]. This should be used to initialize clients or for any other
    /// temporal-owned metrics. User defined metrics should use [Self::get_metric_meter].
    pub fn get_temporal_metric_meter(&self) -> Option<TemporalMeter> {
        self.metrics.clone().map(|m| {
            let kvs = self.default_kvs();
            let attribs = NewAttributes::new(kvs);
            TemporalMeter::new(
                Arc::new(PrefixedMetricsMeter::new(self.metric_prefix.clone(), m))
                    as Arc<dyn CoreMeter>,
                attribs,
            )
        })
    }

    /// Returns our wrapper for metric meters, including attaching the service name if enabled.
    pub fn get_metric_meter(&self) -> Option<TemporalMeter> {
        self.metrics.clone().map(|m| {
            let kvs = self.default_kvs();
            let attribs = NewAttributes::new(kvs);
            TemporalMeter::new(m, attribs)
        })
    }

    fn default_kvs(&self) -> Vec<MetricKeyValue> {
        if self.attach_service_name {
            vec![MetricKeyValue::new("service_name", TELEM_SERVICE_NAME)]
        } else {
            vec![]
        }
    }
}

thread_local! {
    static SUB_GUARD: RefCell<Option<tracing::subscriber::DefaultGuard>> =
        const { RefCell::new(None) };
}
/// Set the trace subscriber for the current thread. This must be done in every thread which uses
/// core stuff, otherwise traces/logs will not be collected on that thread. For example, if using
/// a multithreaded Tokio runtime, you should ensure that said runtime uses
/// [on_thread_start](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.on_thread_start)
/// or a similar mechanism to call this for each thread within the runtime.
pub fn set_trace_subscriber_for_current_thread(sub: impl Subscriber + Send + Sync + 'static) {
    SUB_GUARD.with(|sg| {
        if sg.borrow().is_none() {
            let g = tracing::subscriber::set_default(sub);
            *sg.borrow_mut() = Some(g);
        }
    })
}

/// Undoes [set_trace_subscriber_for_current_thread]
pub fn remove_trace_subscriber_for_current_thread() {
    SUB_GUARD.take();
}

impl CoreTelemetry for TelemetryInstance {
    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        if let Some(logs_out) = self.logs_out.as_ref() {
            logs_out.lock().drain()
        } else {
            vec![]
        }
    }
}

/// Initialize tracing subscribers/output and logging export, returning a [TelemetryInstance]
/// which can be used to register default / global tracing subscribers.
///
/// You should only call this once per unique [TelemetryOptions]
///
/// See [TelemetryOptions] docs for more on configuration.
pub fn telemetry_init(opts: TelemetryOptions) -> Result<TelemetryInstance, anyhow::Error> {
    let mut logs_out = None;

    // Tracing subscriber layers =========
    let mut console_pretty_layer = None;
    let mut console_compact_layer = None;
    let mut forward_layer = None;
    // ===================================

    let tracing_sub = if let Some(ts) = opts.subscriber_override {
        Some(ts)
    } else {
        opts.logging.map(|logger| {
            match logger {
                Logger::Console { filter } => {
                    // This is silly dupe but can't be avoided without boxing.
                    if env::var("TEMPORAL_CORE_PRETTY_LOGS").is_ok() {
                        console_pretty_layer = Some(
                            tracing_subscriber::fmt::layer()
                                .with_target(false)
                                .event_format(
                                    tracing_subscriber::fmt::format()
                                        .pretty()
                                        .with_source_location(false),
                                )
                                .with_filter(EnvFilter::new(filter)),
                        )
                    } else {
                        console_compact_layer = Some(
                            tracing_subscriber::fmt::layer()
                                .with_target(false)
                                .event_format(
                                    tracing_subscriber::fmt::format()
                                        .compact()
                                        .with_source_location(false),
                                )
                                .with_filter(EnvFilter::new(filter)),
                        )
                    }
                }
                Logger::Forward { filter } => {
                    let (export_layer, lo) =
                        CoreLogConsumerLayer::new_buffered(FORWARD_LOG_BUFFER_SIZE);
                    logs_out = Some(Mutex::new(lo));
                    forward_layer = Some(export_layer.with_filter(EnvFilter::new(filter)));
                }
                Logger::Push { filter, consumer } => {
                    forward_layer = Some(
                        CoreLogConsumerLayer::new(consumer).with_filter(EnvFilter::new(filter)),
                    );
                }
            };
            let reg = tracing_subscriber::registry()
                .with(console_pretty_layer)
                .with(console_compact_layer)
                .with(forward_layer);

            #[cfg(feature = "tokio-console")]
            let reg = reg.with(console_subscriber::spawn());
            Arc::new(reg) as Arc<dyn Subscriber + Send + Sync>
        })
    };

    Ok(TelemetryInstance::new(
        tracing_sub,
        logs_out,
        opts.metric_prefix,
        opts.metrics,
        opts.attach_service_name,
    ))
}

/// WARNING: Calling can cause panics because of https://github.com/tokio-rs/tracing/issues/1656
/// Lang must not start using until resolved
///
/// Initialize telemetry/tracing globally. Useful for testing. Only takes affect when called
/// the first time. Subsequent calls are ignored.
pub fn telemetry_init_global(opts: TelemetryOptions) -> Result<(), anyhow::Error> {
    static INITTED: AtomicBool = AtomicBool::new(false);
    if INITTED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        let ti = telemetry_init(opts)?;
        if let Some(ts) = ti.trace_subscriber() {
            tracing::subscriber::set_global_default(ts)?;
        }
    }
    Ok(())
}

/// WARNING: Calling can cause panics because of https://github.com/tokio-rs/tracing/issues/1656
/// Lang must not start using until resolved
///
/// Initialize the fallback global handler. All lang SDKs should call this somewhere, once, at
/// startup, as it initializes a fallback handler for any dependencies (looking at you, otel) that
/// don't provide good ways to customize their tracing usage. It sets a WARN-level global filter
/// that uses the default console logger.
pub fn telemetry_init_fallback() -> Result<(), anyhow::Error> {
    telemetry_init_global(
        TelemetryOptionsBuilder::default()
            .logging(Logger::Console {
                filter: construct_filter_string(Level::DEBUG, Level::WARN),
            })
            .build()?,
    )?;
    Ok(())
}

/// A trait for using [Display] on the contents of vecs, etc, which don't implement it.
///
/// Dislike this, but, there doesn't seem to be a great alternative. Calling itertools format
/// inline in an `event!` macro can panic because it gets evaluated twice somehow.
pub(crate) trait VecDisplayer {
    fn display(&self) -> String;
}

impl<T> VecDisplayer for Vec<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        format!("[{}]", self.iter().format(","))
    }
}

impl<T> VecDisplayer for VecDeque<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        format!("[{}]", self.iter().format(","))
    }
}
