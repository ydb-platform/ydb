use crate::{
    ByteArray, ByteArrayRef, NewlineDelimitedMapRef,
    metric::{CustomMetricMeter, CustomMetricMeterRef},
};

use serde_json::json;
use std::{
    collections::HashMap,
    fmt,
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, UNIX_EPOCH},
};
use temporalio_common::{
    protos::temporal::api::worker::v1::environment_info::{
        Runtime as CoreEnvironmentRuntime, runtime::RuntimeType as CoreRuntimeType,
    },
    telemetry::{
        CoreLog, CoreLogConsumer, HistogramBucketOverrides, Logger, MetricTemporality,
        OtelCollectorOptions, PrometheusExporterOptions, TelemetryOptions as CoreTelemetryOptions,
        build_otlp_metric_exporter, metrics::CoreMeter, start_prometheus_metric_exporter,
    },
};
use temporalio_sdk_core::{
    CoreRuntime, RuntimeOptions as CoreRuntimeOptions, TokioRuntimeBuilder as TokioRuntime,
};
use tracing::Level;
use url::Url;

#[repr(C)]
pub struct RuntimeOptions {
    pub telemetry: *const TelemetryOptions,
    pub worker_heartbeat_interval_millis: u64,
    /// SDK runtimes included in worker environment heartbeats. An empty array reports no runtime;
    /// the bridge does not inherit Core's native-runtime default.
    pub runtime_info: RuntimeInfoArray,
    /// If true, worker heartbeats omit all runtime, hosting, and platform information.
    pub disable_environment_info: bool,
}

#[derive(Clone, Copy)]
#[repr(C)]
pub enum RuntimeType {
    Unspecified = 0,
    Jvm = 1,
    Cpython = 2,
    Node = 3,
    Bun = 4,
    Cruby = 5,
    Go = 6,
    DotnetFramework = 7,
    DotnetCore = 8,
    Native = 9,
    Roadrunner = 10,
}

#[repr(C)]
pub struct RuntimeInfo {
    /// The SDK runtime hosting Core.
    pub runtime_type: RuntimeType,
    /// The runtime version, or empty if it cannot be determined.
    pub version: ByteArrayRef,
}

#[repr(C)]
pub struct RuntimeInfoArray {
    /// Runtime entries read during runtime construction; ownership remains with the caller.
    pub data: *const RuntimeInfo,
    pub size: libc::size_t,
}

impl RuntimeInfoArray {
    fn to_core_runtimes(&self) -> Vec<CoreEnvironmentRuntime> {
        if self.size == 0 {
            return Vec::new();
        }

        unsafe { std::slice::from_raw_parts(self.data, self.size) }
            .iter()
            .filter_map(|runtime| {
                let runtime_type = match runtime.runtime_type {
                    RuntimeType::Unspecified => return None,
                    RuntimeType::Jvm => CoreRuntimeType::Jvm,
                    RuntimeType::Cpython => CoreRuntimeType::Cpython,
                    RuntimeType::Node => CoreRuntimeType::Node,
                    RuntimeType::Bun => CoreRuntimeType::Bun,
                    RuntimeType::Cruby => CoreRuntimeType::Cruby,
                    RuntimeType::Go => CoreRuntimeType::Go,
                    RuntimeType::DotnetFramework => CoreRuntimeType::DotnetFramework,
                    RuntimeType::DotnetCore => CoreRuntimeType::DotnetCore,
                    RuntimeType::Native => CoreRuntimeType::Native,
                    RuntimeType::Roadrunner => CoreRuntimeType::Roadrunner,
                };
                Some(CoreEnvironmentRuntime {
                    r#type: runtime_type as i32,
                    version: runtime.version.to_option_string().unwrap_or_default(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_runtime_version_with_null_data_is_supported() {
        let runtime_info = [RuntimeInfo {
            runtime_type: RuntimeType::Native,
            version: ByteArrayRef {
                data: std::ptr::null(),
                size: 0,
            },
        }];
        let runtimes = RuntimeInfoArray {
            data: runtime_info.as_ptr(),
            size: runtime_info.len(),
        }
        .to_core_runtimes();

        assert_eq!(runtimes.len(), 1);
        assert_eq!(runtimes[0].r#type, CoreRuntimeType::Native as i32);
        assert!(runtimes[0].version.is_empty());
    }
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
    pub global_tags: NewlineDelimitedMapRef,
    pub metric_prefix: ByteArrayRef,
}

#[repr(C)]
pub struct OpenTelemetryOptions {
    pub url: ByteArrayRef,
    pub headers: NewlineDelimitedMapRef,
    pub metric_periodicity_millis: u32,
    pub metric_temporality: OpenTelemetryMetricTemporality,
    pub durations_as_seconds: bool,
    pub protocol: OpenTelemetryProtocol,
    /// Histogram bucket overrides in form of
    /// `<metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>`
    pub histogram_bucket_overrides: NewlineDelimitedMapRef,
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
    /// `<metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>`
    pub histogram_bucket_overrides: NewlineDelimitedMapRef,
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
                    CoreRuntime::new(CoreRuntimeOptions::default(), TokioRuntime::default())
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
            let (attach_service_name, metric_prefix) =
                if let Some(v) = unsafe { v.metrics.as_ref() } {
                    (v.attach_service_name, v.metric_prefix.to_option_string())
                } else {
                    (true, None)
                };

            let logging = unsafe { v.logging.as_ref() }.map(|v| {
                if let Some(callback) = v.forward_to {
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
                        format: None,
                    }
                }
            });

            CoreTelemetryOptions::builder()
                .attach_service_name(attach_service_name)
                .maybe_metric_prefix(metric_prefix)
                .maybe_logging(logging)
                .build()
        } else {
            CoreTelemetryOptions::default()
        };

        let core_runtime_options = CoreRuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .heartbeat_interval(
                (options.worker_heartbeat_interval_millis != 0)
                    .then(|| Duration::from_millis(options.worker_heartbeat_interval_millis)),
            )
            .disable_environment_info(options.disable_environment_info)
            .build()
            .map_err(|e| anyhow::anyhow!(e))?
            .with_runtimes(options.runtime_info.to_core_runtimes());

        // Build core runtime
        let mut core = CoreRuntime::new(core_runtime_options, TokioRuntime::default())?;

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
        Ok(Arc::new(build_otlp_metric_exporter(
            OtelCollectorOptions::builder()
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
                })
                .maybe_metric_periodicity(
                    (otel_options.metric_periodicity_millis > 0).then(|| {
                        Duration::from_millis(otel_options.metric_periodicity_millis.into())
                    }),
                )
                .build(),
        )?))
    } else if let Some(prom_options) = unsafe { options.prometheus.as_ref() } {
        if custom_meter.is_some() {
            return Err(anyhow::anyhow!(
                "Cannot have Prometheus metrics and custom meter"
            ));
        }
        // Start prom exporter
        let build = PrometheusExporterOptions::builder()
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
        Ok(start_prometheus_metric_exporter(build.build())?.meter)
    } else if let Some(custom_meter) = custom_meter {
        Ok(Arc::new(custom_meter))
    } else {
        Err(anyhow::anyhow!(
            "Either OpenTelemetry config, Prometheus config, or custom meter must be provided"
        ))
    }
}

fn parse_histogram_bucket_overrides(
    raw: &NewlineDelimitedMapRef,
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
