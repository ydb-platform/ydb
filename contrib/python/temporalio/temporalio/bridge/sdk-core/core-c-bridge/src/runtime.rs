use crate::ByteArray;
use crate::ByteArrayRef;
use crate::MetadataRef;
use crate::metric::CustomMetricMeter;
use crate::metric::CustomMetricMeterRef;

use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::UNIX_EPOCH;
use temporal_sdk_core::CoreRuntime;
use temporal_sdk_core::TokioRuntimeBuilder;
use temporal_sdk_core::telemetry::{build_otlp_metric_exporter, start_prometheus_metric_exporter};
use temporal_sdk_core_api::telemetry::HistogramBucketOverrides;
use temporal_sdk_core_api::telemetry::MetricTemporality;
use temporal_sdk_core_api::telemetry::metrics::CoreMeter;
use temporal_sdk_core_api::telemetry::{CoreLog, CoreLogConsumer};
use temporal_sdk_core_api::telemetry::{
    Logger, OtelCollectorOptionsBuilder, PrometheusExporterOptionsBuilder,
    TelemetryOptions as CoreTelemetryOptions, TelemetryOptionsBuilder,
};
use tracing::Level;
use url::Url;

#[repr(C)]
pub struct RuntimeOptions {
    pub telemetry: *const TelemetryOptions,
}

#[repr(C)]
pub struct TelemetryOptions {
    pub logging: *const LoggingOptions,
    pub metrics: *const MetricsOptions,
}

#[repr(C)]
pub struct LoggingOptions {
    pub filter: ByteArrayRef,
    /// This callback is expected to work for the life of the runtime.
    pub forward_to: ForwardedLogCallback,
}

// This has to be Option here because of https://github.com/mozilla/cbindgen/issues/326
/// Operations on the log can only occur within the callback, it is freed
/// immediately thereafter.
pub type ForwardedLogCallback =
    Option<unsafe extern "C" fn(level: ForwardedLogLevel, log: *const ForwardedLog)>;

pub struct ForwardedLog {
    core: CoreLog,
    fields_json: Arc<Mutex<Option<String>>>,
}

#[repr(C)]
pub enum ForwardedLogLevel {
    Trace = 0,
    Debug,
    Info,
    Warn,
    Error,
}

/// Only one of opentelemetry, prometheus, or custom_meter can be present.
#[repr(C)]
pub struct MetricsOptions {
    pub opentelemetry: *const OpenTelemetryOptions,
    pub prometheus: *const PrometheusOptions,
    /// If present, this is freed by a callback within itself
    pub custom_meter: *const CustomMetricMeter,

    pub attach_service_name: bool,
    pub global_tags: MetadataRef,
    pub metric_prefix: ByteArrayRef,
}

#[repr(C)]
pub struct OpenTelemetryOptions {
    pub url: ByteArrayRef,
    pub headers: MetadataRef,
    pub metric_periodicity_millis: u32,
    pub metric_temporality: OpenTelemetryMetricTemporality,
    pub durations_as_seconds: bool,
    pub protocol: OpenTelemetryProtocol,
    /// Histogram bucket overrides in form of
    /// <metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>
    pub histogram_bucket_overrides: MetadataRef,
}

#[repr(C)]
pub enum OpenTelemetryMetricTemporality {
    Cumulative = 1,
    Delta,
}

#[repr(C)]
pub enum OpenTelemetryProtocol {
    Grpc = 1,
    Http,
}

#[repr(C)]
pub struct PrometheusOptions {
    pub bind_address: ByteArrayRef,
    pub counters_total_suffix: bool,
    pub unit_suffix: bool,
    pub durations_as_seconds: bool,
    /// Histogram bucket overrides in form of
    /// <metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>
    pub histogram_bucket_overrides: MetadataRef,
}

#[derive(Clone)]
pub struct Runtime {
    pub(crate) core: Arc<CoreRuntime>,
    log_forwarder: Option<Arc<LogForwarder>>,
}

/// If fail is not null, it must be manually freed when done. Runtime is always
/// present, but it should never be used if fail is present, only freed after
/// fail is freed using it.
#[repr(C)]
pub struct RuntimeOrFail {
    pub runtime: *mut Runtime,
    pub fail: *const ByteArray,
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_runtime_new(options: *const RuntimeOptions) -> RuntimeOrFail {
    match Runtime::new(unsafe { &*options }) {
        Ok(runtime) => RuntimeOrFail {
            runtime: Box::into_raw(Box::new(runtime)),
            fail: std::ptr::null(),
        },
        Err(err) => {
            // We have to make an empty runtime just for the failure to be
            // freeable
            let mut runtime = Runtime {
                core: Arc::new(
                    CoreRuntime::new(
                        CoreTelemetryOptions::default(),
                        TokioRuntimeBuilder::default(),
                    )
                    .unwrap(),
                ),
                log_forwarder: None,
            };
            let fail = runtime.alloc_utf8(&format!("Invalid options: {err}"));
            RuntimeOrFail {
                runtime: Box::into_raw(Box::new(runtime)),
                fail: fail.into_raw(),
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_runtime_free(runtime: *mut Runtime) {
    unsafe {
        let _ = Box::from_raw(runtime);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_byte_array_free(runtime: *mut Runtime, bytes: *const ByteArray) {
    // Bail if freeing is disabled
    unsafe {
        if bytes.is_null() || (*bytes).disable_free {
            return;
        }
    }
    let bytes = bytes as *mut ByteArray;
    // Return vec back to core before dropping bytes
    let vec = unsafe { Vec::from_raw_parts((*bytes).data as *mut u8, (*bytes).size, (*bytes).cap) };
    // Set to null so the byte dropper doesn't try to free it
    unsafe { (*bytes).data = std::ptr::null_mut() };
    // Return only if runtime is non-null
    if !runtime.is_null() {
        let runtime = unsafe { &mut *runtime };
        runtime.return_buf(vec);
    }
    unsafe {
        let _ = Box::from_raw(bytes);
    }
}

impl Runtime {
    fn new(options: &RuntimeOptions) -> anyhow::Result<Runtime> {
        // Create custom meter here so it will be dropped on any error and
        // therefore will call "free"
        let custom_meter = unsafe {
            options
                .telemetry
                .as_ref()
                .and_then(|v| v.metrics.as_ref())
                .map(|v| v.custom_meter)
                .filter(|v| !v.is_null())
                .map(CustomMetricMeterRef::new)
        };

        // Build telemetry options
        let mut log_forwarder = None;
        let telemetry_options = if let Some(v) = unsafe { options.telemetry.as_ref() } {
            let mut build = TelemetryOptionsBuilder::default();

            // Metrics options (note, metrics meter is late-bound later)
            if let Some(v) = unsafe { v.metrics.as_ref() } {
                build.attach_service_name(v.attach_service_name);
                if let Some(metric_prefix) = v.metric_prefix.to_option_string() {
                    build.metric_prefix(metric_prefix);
                }
            }

            // Logging options
            if let Some(v) = unsafe { v.logging.as_ref() } {
                build.logging(if let Some(callback) = v.forward_to {
                    let consumer = Arc::new(LogForwarder {
                        callback,
                        active: AtomicBool::new(false),
                    });
                    log_forwarder = Some(consumer.clone());
                    Logger::Push {
                        filter: v.filter.to_string(),
                        consumer,
                    }
                } else {
                    Logger::Console {
                        filter: v.filter.to_string(),
                    }
                });
            }
            build.build()?
        } else {
            CoreTelemetryOptions::default()
        };

        // Build core runtime
        let mut core = CoreRuntime::new(telemetry_options, TokioRuntimeBuilder::default())?;

        // We late-bind the metrics after core runtime is created since it needs
        // the Tokio handle
        if let Some(v) = unsafe { options.telemetry.as_ref() }
            && let Some(v) = unsafe { v.metrics.as_ref() }
        {
            let _guard = core.tokio_handle().enter();
            core.telemetry_mut()
                .attach_late_init_metrics(create_meter(v, custom_meter)?);
        }

        // Create runtime
        let runtime = Runtime {
            core: Arc::new(core),
            log_forwarder,
        };

        // Set log forwarder to active. We do this later so logs don't get
        // inadvertently sent if this errors above.
        if let Some(log_forwarder) = runtime.log_forwarder.as_ref() {
            log_forwarder.active.store(true, Ordering::Release);
        }

        Ok(runtime)
    }

    fn borrow_buf(&mut self) -> Vec<u8> {
        // We currently do not use a thread-safe byte pool, but if wanted, it
        // can be added here
        Vec::new()
    }

    fn return_buf(&mut self, _vec: Vec<u8>) {
        // We currently do not use a thread-safe byte pool, but if wanted, it
        // can be added here
    }

    pub fn alloc_utf8(&mut self, v: &str) -> ByteArray {
        let mut buf = self.borrow_buf();
        buf.clear();
        buf.extend_from_slice(v.as_bytes());
        ByteArray::from_vec(buf)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(log_forwarder) = self.log_forwarder.as_ref() {
            // Need strong guarantees to ensure the callback is not called again
            // after this drop completes
            log_forwarder.active.store(false, Ordering::Release);
        }
    }
}

struct LogForwarder {
    callback: unsafe extern "C" fn(level: ForwardedLogLevel, log: *const ForwardedLog),
    active: AtomicBool,
}

impl CoreLogConsumer for LogForwarder {
    fn on_log(&self, log: CoreLog) {
        // Check whether active w/ strong consistency
        if self.active.load(Ordering::Acquire) {
            let level = match log.level {
                Level::TRACE => ForwardedLogLevel::Trace,
                Level::DEBUG => ForwardedLogLevel::Debug,
                Level::INFO => ForwardedLogLevel::Info,
                Level::WARN => ForwardedLogLevel::Warn,
                Level::ERROR => ForwardedLogLevel::Error,
            };
            // Create log here to live the life of the callback
            let log = ForwardedLog {
                core: log,
                fields_json: Arc::new(Mutex::new(None)),
            };
            unsafe { (self.callback)(level, &log) };
        }
    }
}

impl fmt::Debug for LogForwarder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<log forwarder>")
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_forwarded_log_target(log: *const ForwardedLog) -> ByteArrayRef {
    let log = unsafe { &*log };
    log.core.target.as_str().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_forwarded_log_message(log: *const ForwardedLog) -> ByteArrayRef {
    let log = unsafe { &*log };
    log.core.message.as_str().into()
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_forwarded_log_timestamp_millis(log: *const ForwardedLog) -> u64 {
    let log = unsafe { &*log };
    log.core
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

#[unsafe(no_mangle)]
pub extern "C" fn temporal_core_forwarded_log_fields_json(
    log: *const ForwardedLog,
) -> ByteArrayRef {
    let log = unsafe { &*log };
    // If not set, we convert to JSON under lock then set
    let fields_json = log.fields_json.clone();
    let mut fields_json = fields_json.lock().unwrap();
    fields_json
        .get_or_insert_with(|| json!(&log.core.fields).to_string())
        .as_str()
        .into()
}

fn create_meter(
    options: &MetricsOptions,
    custom_meter: Option<CustomMetricMeterRef>,
) -> anyhow::Result<Arc<dyn CoreMeter>> {
    // OTel, Prom, or custom
    if let Some(otel_options) = unsafe { options.opentelemetry.as_ref() } {
        if !options.prometheus.is_null() || custom_meter.is_some() {
            return Err(anyhow::anyhow!(
                "Cannot have OpenTelemetry and Prometheus metrics or custom meter"
            ));
        }
        // Build OTel exporter
        let mut build = OtelCollectorOptionsBuilder::default();
        build
            .url(Url::parse(otel_options.url.to_str())?)
            .headers(otel_options.headers.to_string_map_on_newlines())
            .metric_temporality(match otel_options.metric_temporality {
                OpenTelemetryMetricTemporality::Cumulative => MetricTemporality::Cumulative,
                OpenTelemetryMetricTemporality::Delta => MetricTemporality::Delta,
            })
            .global_tags(options.global_tags.to_string_map_on_newlines())
            .use_seconds_for_durations(otel_options.durations_as_seconds)
            .histogram_bucket_overrides(HistogramBucketOverrides {
                overrides: parse_histogram_bucket_overrides(
                    &otel_options.histogram_bucket_overrides,
                )?,
            });
        if otel_options.metric_periodicity_millis > 0 {
            build.metric_periodicity(Duration::from_millis(
                otel_options.metric_periodicity_millis.into(),
            ));
        }
        Ok(Arc::new(build_otlp_metric_exporter(build.build()?)?))
    } else if let Some(prom_options) = unsafe { options.prometheus.as_ref() } {
        if custom_meter.is_some() {
            return Err(anyhow::anyhow!(
                "Cannot have Prometheus metrics and custom meter"
            ));
        }
        // Start prom exporter
        let mut build = PrometheusExporterOptionsBuilder::default();
        build
            .socket_addr(SocketAddr::from_str(prom_options.bind_address.to_str())?)
            .global_tags(options.global_tags.to_string_map_on_newlines())
            .counters_total_suffix(prom_options.counters_total_suffix)
            .unit_suffix(prom_options.unit_suffix)
            .use_seconds_for_durations(prom_options.durations_as_seconds)
            .histogram_bucket_overrides(HistogramBucketOverrides {
                overrides: parse_histogram_bucket_overrides(
                    &prom_options.histogram_bucket_overrides,
                )?,
            });
        Ok(start_prometheus_metric_exporter(build.build()?)?.meter)
    } else if let Some(custom_meter) = custom_meter {
        Ok(Arc::new(custom_meter))
    } else {
        Err(anyhow::anyhow!(
            "Either OpenTelemetry config, Prometheus config, or custom meter must be provided"
        ))
    }
}

fn parse_histogram_bucket_overrides(
    raw: &MetadataRef,
) -> anyhow::Result<HashMap<String, Vec<f64>>> {
    raw.to_string_map_on_newlines()
        .into_iter()
        .map(|(k, v)| {
            let vals: anyhow::Result<Vec<f64>> = v
                .split(',')
                .map(str::parse::<f64>)
                .collect::<Result<_, _>>() // Result<Vec<f64>, ParseFloatError>
                .map_err(Into::into);
            vals.map(|vals| (k, vals))
        })
        .collect()
}
