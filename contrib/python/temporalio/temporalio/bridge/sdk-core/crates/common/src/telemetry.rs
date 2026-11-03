//! Contains tracing/logging and metrics related functionality

/// Metric instrument types and the [`CoreMeter`] trait.
pub mod metrics;

#[cfg(feature = "core-telemetry-bridge")]
mod log_export;
#[cfg(feature = "otel")]
mod otel;
#[cfg(feature = "prometheus")]
mod prometheus_meter;
#[cfg(feature = "prometheus")]
mod prometheus_server;

use crate::telemetry::metrics::{
    CoreMeter, MetricKeyValue, NewAttributes, PrefixedMetricsMeter, TemporalMeter,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    env,
    fmt::{Debug, Formatter},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{Level, Subscriber};
use tracing_subscriber::{EnvFilter, Layer, fmt::MakeWriter, layer::SubscriberExt};
use url::Url;

#[cfg(feature = "core-telemetry-bridge")]
use crate::telemetry::log_export::CoreLogConsumerLayer;

#[cfg(feature = "core-telemetry-bridge")]
pub use log_export::{CoreLogBuffer, CoreLogBufferedConsumer, CoreLogStreamConsumer};
#[cfg(feature = "otel")]
pub use otel::build_otlp_metric_exporter;
#[cfg(feature = "prometheus")]
pub use prometheus_server::start_prometheus_metric_exporter;

/// The default prefix applied to all Temporal metric names.
pub static METRIC_PREFIX: &str = "temporal_";

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";

/// Each core runtime instance has a telemetry subsystem associated with it, this trait defines the
/// operations that lang might want to perform on that telemetry after it's initialized.
pub trait CoreTelemetry {
    /// Each worker buffers logs that should be shuttled over to lang so that they may be rendered
    /// with the user's desired logging library. Use this function to grab the most recent buffered
    /// logs since the last time it was called. A fixed number of such logs are retained at maximum,
    /// with the oldest being dropped when full.
    ///
    /// Returns the list of logs from oldest to newest. Returns an empty vec if the feature is not
    /// configured.
    fn fetch_buffered_logs(&self) -> Vec<CoreLog>;
}

/// Telemetry configuration options. Construct with [TelemetryOptions::builder]
#[derive(Clone, bon::Builder)]
#[non_exhaustive]
pub struct TelemetryOptions {
    /// Optional logger - set as None to disable.
    #[builder(into)]
    pub logging: Option<Logger>,
    /// Optional metrics exporter - set as None to disable.
    #[builder(into)]
    pub metrics: Option<Arc<dyn CoreMeter>>,
    /// If set true (the default) explicitly attach a `service_name` label to all metrics. Turn this
    /// off if your collection system supports the `target_info` metric from the OpenMetrics spec.
    /// For more, see
    /// [here](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#supporting-target-metadata-in-both-push-based-and-pull-based-systems)
    #[builder(default = true)]
    pub attach_service_name: bool,
    /// A prefix to be applied to all core-created metrics. Defaults to "temporal_".
    #[builder(default = METRIC_PREFIX.to_string())]
    pub metric_prefix: String,
    /// If provided, logging config will be ignored and this explicit subscriber will be used for
    /// all logging and traces.
    pub subscriber_override: Option<Arc<dyn Subscriber + Send + Sync>>,
    /// See [TaskQueueLabelStrategy].
    #[builder(default = TaskQueueLabelStrategy::UseNormal)]
    pub task_queue_label_strategy: TaskQueueLabelStrategy,
}
impl Debug for TelemetryOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct TelemetryOptions<'a> {
            logging: &'a Option<Logger>,
            metrics: &'a Option<Arc<dyn CoreMeter>>,
            attach_service_name: &'a bool,
            metric_prefix: &'a str,
        }
        let Self {
            logging,
            metrics,
            attach_service_name,
            metric_prefix,
            ..
        } = self;

        Debug::fmt(
            &TelemetryOptions {
                logging,
                metrics,
                attach_service_name,
                metric_prefix,
            },
            f,
        )
    }
}

/// Determines how the `task_queue` label value is set on metrics.
#[derive(Copy, Clone, Debug)]
#[non_exhaustive]
pub enum TaskQueueLabelStrategy {
    /// Always use the normal task queue name, including for actions relating to sticky queues.
    UseNormal,
    /// Use the sticky queue name when recording metrics operating on sticky queues.
    UseNormalAndSticky,
}

/// Options for exporting to an OpenTelemetry Collector
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct OtelCollectorOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector.
    pub url: Url,
    /// Optional set of HTTP headers to send to the Collector, e.g for authentication.
    #[builder(default = HashMap::new())]
    pub headers: HashMap<String, String>,
    /// Optionally specify how frequently metrics should be exported. Defaults to 1 second.
    #[builder(default = Duration::from_secs(1))]
    pub metric_periodicity: Duration,
    /// Specifies the aggregation temporality for metric export. Defaults to cumulative.
    #[builder(default = MetricTemporality::Cumulative)]
    pub metric_temporality: MetricTemporality,
    /// A map of tags to be applied to all metrics
    #[builder(default)]
    pub global_tags: HashMap<String, String>,
    /// If set to true, use f64 seconds for durations instead of u64 milliseconds
    #[builder(default)]
    pub use_seconds_for_durations: bool,
    /// Overrides for histogram buckets. Units depend on the value of `use_seconds_for_durations`.
    #[builder(default)]
    pub histogram_bucket_overrides: HistogramBucketOverrides,
    /// Protocol to use for communication with the collector
    #[builder(default = OtlpProtocol::Grpc)]
    pub protocol: OtlpProtocol,
}

/// Options for exporting metrics to Prometheus
#[derive(Debug, Clone, bon::Builder)]
#[non_exhaustive]
pub struct PrometheusExporterOptions {
    /// The address the Prometheus exporter HTTP server will bind to.
    pub socket_addr: SocketAddr,
    /// A map of tags to be applied to all metrics
    #[builder(default)]
    pub global_tags: HashMap<String, String>,
    /// If set true, all counters will include a "_total" suffix
    #[builder(default)]
    pub counters_total_suffix: bool,
    /// If set true, all histograms will include the unit in their name as a suffix.
    /// Ex: "_milliseconds".
    #[builder(default)]
    pub unit_suffix: bool,
    /// If set to true, use f64 seconds for durations instead of u64 milliseconds
    #[builder(default)]
    pub use_seconds_for_durations: bool,
    /// Overrides for histogram buckets. Units depend on the value of `use_seconds_for_durations`.
    #[builder(default)]
    pub histogram_bucket_overrides: HistogramBucketOverrides,
}

/// Allows overriding the buckets used by histogram metrics
#[derive(Debug, Clone, Default)]
pub struct HistogramBucketOverrides {
    /// Overrides where the key is the metric name and the value is the list of bucket boundaries.
    /// The metric name will apply regardless of name prefixing, if any. IE: the name acts like
    /// `*metric_name`.
    ///
    /// The string names of core's built-in histogram metrics are publicly available on the
    /// `core::telemetry` module and the `client` crate.
    ///
    /// See [here](https://docs.rs/opentelemetry_sdk/latest/opentelemetry_sdk/metrics/enum.Aggregation.html#variant.ExplicitBucketHistogram.field.boundaries)
    /// for the exact meaning of boundaries.
    pub overrides: HashMap<String, Vec<f64>>,
}

/// Control where logs go
#[derive(Debug, Clone)]
pub enum Logger {
    /// Log directly to console.
    Console {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
        /// The format used to render logs. When unset, `TEMPORAL_CORE_PRETTY_LOGS` selects pretty
        /// output if present; otherwise logs use the compact format.
        format: Option<LoggerFormat>,
    },
    #[cfg(feature = "core-telemetry-bridge")]
    /// Forward logs to Lang - collectable with `fetch_buffered_logs`.
    Forward {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
    },
    #[cfg(feature = "core-telemetry-bridge")]
    /// Push logs to Lang. Can be used with
    /// temporalio_sdk_core::telemetry::log_export::CoreLogBufferedConsumer to buffer.
    Push {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
        /// Trait invoked on each log.
        consumer: Arc<dyn CoreLogConsumer>,
    },
}

/// Controls how console logs are rendered.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub enum LoggerFormat {
    /// Render logs compactly on a single line.
    #[default]
    Compact,
    /// Render logs in a human-readable format across multiple lines.
    Pretty,
    /// Render each log as a JSON object on a single line.
    Json,
}

/// Types of aggregation temporality for metric export.
/// See: <https://github.com/open-telemetry/opentelemetry-specification/blob/ce50e4634efcba8da445cc23523243cb893905cb/specification/metrics/datamodel.md#temporality>
#[derive(Debug, Clone, Copy)]
pub enum MetricTemporality {
    /// Successive data points repeat the starting timestamp
    Cumulative,
    /// Successive data points advance the starting timestamp
    Delta,
}

/// Options for configuring telemetry
#[derive(Debug, Clone, Copy)]
pub enum OtlpProtocol {
    /// Use gRPC to communicate with the collector
    Grpc,
    /// Use HTTP to communicate with the collector
    Http,
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        TelemetryOptions::builder().build()
    }
}

/// A log line (which ultimately came from a tracing event) exported from Core->Lang
#[derive(Debug)]
pub struct CoreLog {
    /// The module within core this message originated from
    pub target: String,
    /// Log message
    pub message: String,
    /// Time log was generated (not when it was exported to lang)
    pub timestamp: SystemTime,
    /// Message level
    pub level: Level,
    /// Arbitrary k/v pairs (span k/vs are collapsed with event k/vs here). We could change this
    /// to include them in `span_contexts` instead, but there's probably not much value for log
    /// forwarding.
    pub fields: HashMap<String, serde_json::Value>,
    /// A list of the outermost to the innermost span names
    pub span_contexts: Vec<String>,
}

impl CoreLog {
    /// Return timestamp as ms since epoch
    pub fn millis_since_epoch(&self) -> u128 {
        self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    }
}

/// Consumer trait for use with push logger.
pub trait CoreLogConsumer: Send + Sync + Debug {
    /// Invoked synchronously for every single log.
    fn on_log(&self, log: CoreLog);
}

#[cfg(feature = "core-telemetry-bridge")]
const FORWARD_LOG_BUFFER_SIZE: usize = 2048;

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_level`.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{other_level},temporalio_common={core_level},temporalio_sdk_core={core_level},temporalio_client={core_level},temporalio_sdk={core_level}"
    )
}

/// Holds initialized tracing/metrics exporters, etc
pub struct TelemetryInstance {
    metric_prefix: String,
    #[cfg(feature = "core-telemetry-bridge")]
    logs_out: Option<parking_lot::Mutex<CoreLogBuffer>>,
    metrics: Option<Arc<dyn CoreMeter + 'static>>,
    /// The tracing subscriber which is associated with this telemetry instance. May be `None` if
    /// the user has not opted into any tracing configuration.
    trace_subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,
    attach_service_name: bool,
    task_queue_label_strategy: TaskQueueLabelStrategy,
}

impl TelemetryInstance {
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
                self.task_queue_label_strategy,
            )
        })
    }

    /// Returns our wrapper for metric meters, including attaching the service name if enabled.
    pub fn get_metric_meter(&self) -> Option<TemporalMeter> {
        self.metrics.clone().map(|m| {
            let kvs = self.default_kvs();
            let attribs = NewAttributes::new(kvs);
            TemporalMeter::new(m, attribs, self.task_queue_label_strategy)
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

#[cfg(feature = "core-telemetry-bridge")]
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
    telemetry_init_with_console_writer(
        opts,
        std::io::stdout,
        env::var("TEMPORAL_CORE_PRETTY_LOGS").is_ok(),
    )
}

fn telemetry_init_with_console_writer<W>(
    opts: TelemetryOptions,
    console_writer: W,
    pretty_logs_env_set: bool,
) -> Result<TelemetryInstance, anyhow::Error>
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    #[cfg(feature = "core-telemetry-bridge")]
    let mut logs_out = None;

    // Tracing subscriber layers =========
    let mut console_pretty_layer = None;
    let mut console_compact_layer = None;
    let mut console_json_layer = None;
    #[cfg(feature = "core-telemetry-bridge")]
    let mut forward_layer = None;
    // ===================================

    let tracing_sub = if let Some(ts) = opts.subscriber_override {
        Some(ts)
    } else {
        opts.logging.map(|logger| {
            match logger {
                Logger::Console { filter, format } => {
                    // This is silly dupe but can't be avoided without boxing.
                    let format = format.unwrap_or(if pretty_logs_env_set {
                        LoggerFormat::Pretty
                    } else {
                        LoggerFormat::Compact
                    });
                    match format {
                        LoggerFormat::Pretty => {
                            console_pretty_layer = Some(
                                tracing_subscriber::fmt::layer()
                                    .with_writer(console_writer)
                                    .with_target(false)
                                    .event_format(
                                        tracing_subscriber::fmt::format()
                                            .pretty()
                                            .with_source_location(false),
                                    )
                                    .with_filter(EnvFilter::new(filter)),
                            )
                        }
                        LoggerFormat::Compact => {
                            console_compact_layer = Some(
                                tracing_subscriber::fmt::layer()
                                    .with_writer(console_writer)
                                    .with_target(false)
                                    .event_format(
                                        tracing_subscriber::fmt::format()
                                            .compact()
                                            .with_source_location(false),
                                    )
                                    .with_filter(EnvFilter::new(filter)),
                            )
                        }
                        LoggerFormat::Json => {
                            console_json_layer = Some(
                                tracing_subscriber::fmt::layer()
                                    .with_writer(console_writer)
                                    .with_target(false)
                                    .json()
                                    .with_filter(EnvFilter::new(filter)),
                            )
                        }
                    }
                }
                #[cfg(feature = "core-telemetry-bridge")]
                Logger::Forward { filter } => {
                    let (export_layer, lo) =
                        CoreLogConsumerLayer::new_buffered(FORWARD_LOG_BUFFER_SIZE);
                    logs_out = Some(parking_lot::Mutex::new(lo));
                    forward_layer = Some(export_layer.with_filter(EnvFilter::new(filter)));
                }
                #[cfg(feature = "core-telemetry-bridge")]
                Logger::Push { filter, consumer } => {
                    forward_layer = Some(
                        CoreLogConsumerLayer::new(consumer).with_filter(EnvFilter::new(filter)),
                    );
                }
            };
            let reg = tracing_subscriber::registry()
                .with(console_pretty_layer)
                .with(console_compact_layer)
                .with(console_json_layer);
            #[cfg(feature = "core-telemetry-bridge")]
            let reg = reg.with(forward_layer);

            Arc::new(reg) as Arc<dyn Subscriber + Send + Sync>
        })
    };

    Ok(TelemetryInstance {
        metric_prefix: opts.metric_prefix,
        #[cfg(feature = "core-telemetry-bridge")]
        logs_out,
        metrics: opts.metrics,
        trace_subscriber: tracing_sub,
        attach_service_name: opts.attach_service_name,
        task_queue_label_strategy: opts.task_queue_label_strategy,
    })
}

/// WARNING: Calling can cause panics because of <https://github.com/tokio-rs/tracing/issues/1656>
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

/// WARNING: Calling can cause panics because of <https://github.com/tokio-rs/tracing/issues/1656>
/// Lang must not start using until resolved
///
/// Initialize the fallback global handler. All lang SDKs should call this somewhere, once, at
/// startup, as it initializes a fallback handler for any dependencies (looking at you, otel) that
/// don't provide good ways to customize their tracing usage. It sets a WARN-level global filter
/// that uses the default console logger.
pub fn telemetry_init_fallback() -> Result<(), anyhow::Error> {
    telemetry_init_global(
        TelemetryOptions::builder()
            .logging(Logger::Console {
                filter: construct_filter_string(Level::DEBUG, Level::WARN),
                format: None,
            })
            .build(),
    )?;
    Ok(())
}

/// Ensure a process-wide default rustls `CryptoProvider` is installed.
///
/// Under the `tls-ring` build (and not `tls-aws-lc`), the reqwest clients used for ephemeral
/// server downloads and the OTLP HTTP metric exporter carry no compiled-in crypto provider, so
/// they resolve one from the process default at connection time. Installing ring here lets those
/// clients negotiate TLS without pulling aws-lc-rs into the build. Idempotent, and tolerates a
/// provider having already been installed by another component.
pub fn ensure_default_crypto_provider() {
    #[cfg(all(feature = "tls-ring", not(feature = "tls-aws-lc")))]
    {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct CapturingWriter(Arc<Mutex<Vec<u8>>>);

    impl<'writer> MakeWriter<'writer> for CapturingWriter {
        type Writer = CapturingWriter;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    impl Write for CapturingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn json_console_logs_respect_filter() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let instance = telemetry_init_with_console_writer(
            TelemetryOptions::builder()
                .logging(Logger::Console {
                    filter: "off,json_log_test=info".to_string(),
                    format: Some(LoggerFormat::Json),
                })
                .build(),
            CapturingWriter(output.clone()),
            false,
        )
        .unwrap();
        let subscriber = instance.trace_subscriber().unwrap();

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "json_log_test", answer = 42, "included");
            tracing::debug!(target: "json_log_test", "filtered by level");
            tracing::info!(target: "other_target", "filtered by target");
        });

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        let logs = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0]["fields"]["message"], "included");
        assert_eq!(logs[0]["fields"]["answer"], 42);
    }

    #[test]
    fn explicit_format_overrides_pretty_logs_env() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let instance = telemetry_init_with_console_writer(
            TelemetryOptions::builder()
                .logging(Logger::Console {
                    filter: "info".to_string(),
                    format: Some(LoggerFormat::Json),
                })
                .build(),
            CapturingWriter(output.clone()),
            true,
        )
        .unwrap();
        let subscriber = instance.trace_subscriber().unwrap();

        tracing::subscriber::with_default(subscriber, || tracing::info!("json log"));

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&output).unwrap()["fields"]["message"],
            "json log"
        );
    }

    #[test]
    fn pretty_logs_env_selects_pretty_when_format_unset() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let instance = telemetry_init_with_console_writer(
            TelemetryOptions::builder()
                .logging(Logger::Console {
                    filter: "info".to_string(),
                    format: None,
                })
                .build(),
            CapturingWriter(output.clone()),
            true,
        )
        .unwrap();
        let subscriber = instance.trace_subscriber().unwrap();

        tracing::subscriber::with_default(subscriber, || tracing::info!("pretty log"));

        let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(output.contains("pretty log"));
        assert!(serde_json::from_str::<serde_json::Value>(&output).is_err());
    }
}
