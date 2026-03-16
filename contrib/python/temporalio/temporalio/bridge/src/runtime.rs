use futures::channel::mpsc::Receiver;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pythonize::pythonize;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use temporal_sdk_core::telemetry::{
    build_otlp_metric_exporter, start_prometheus_metric_exporter, CoreLogStreamConsumer,
    MetricsCallBuffer,
};
use temporal_sdk_core::{CoreRuntime, TokioRuntimeBuilder};
use temporal_sdk_core_api::telemetry::metrics::{CoreMeter, MetricCallBufferer};
use temporal_sdk_core_api::telemetry::{
    CoreLog, Logger, MetricTemporality, OtelCollectorOptionsBuilder, OtlpProtocol,
    PrometheusExporterOptionsBuilder, TelemetryOptionsBuilder,
};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::Level;
use url::Url;

use crate::metric::{convert_metric_events, BufferedMetricRef, BufferedMetricUpdate};

#[pyclass]
pub struct RuntimeRef {
    pub(crate) runtime: Runtime,
}

#[derive(Clone)]
pub(crate) struct Runtime {
    pub(crate) core: Arc<CoreRuntime>,
    metrics_call_buffer: Option<Arc<MetricsCallBuffer<BufferedMetricRef>>>,
    log_forwarder_handle: Option<Arc<JoinHandle<()>>>,
}

#[derive(FromPyObject)]
pub struct TelemetryConfig {
    logging: Option<LoggingConfig>,
    metrics: Option<MetricsConfig>,
}

#[derive(FromPyObject)]
pub struct LoggingConfig {
    filter: String,
    forward_to: Option<PyObject>,
}

#[pyclass]
pub struct BufferedLogEntry {
    core_log: CoreLog,
}

#[derive(FromPyObject)]
pub struct MetricsConfig {
    // These fields are mutually exclusive
    opentelemetry: Option<OpenTelemetryConfig>,
    prometheus: Option<PrometheusConfig>,
    buffered_with_size: usize,

    attach_service_name: bool,
    global_tags: Option<HashMap<String, String>>,
    metric_prefix: Option<String>,
}

#[derive(FromPyObject)]
pub struct OpenTelemetryConfig {
    url: String,
    headers: HashMap<String, String>,
    metric_periodicity_millis: Option<u64>,
    metric_temporality_delta: bool,
    durations_as_seconds: bool,
    http: bool,
}

#[derive(FromPyObject)]
pub struct PrometheusConfig {
    bind_address: String,
    counters_total_suffix: bool,
    unit_suffix: bool,
    durations_as_seconds: bool,
    histogram_bucket_overrides: Option<HashMap<String, Vec<f64>>>,
}

const FORWARD_LOG_BUFFER_SIZE: usize = 2048;
const FORWARD_LOG_MAX_FREQ_MS: u64 = 10;

pub fn init_runtime(telemetry_config: TelemetryConfig) -> PyResult<RuntimeRef> {
    // Have to build/start telemetry config pieces
    let mut telemetry_build = TelemetryOptionsBuilder::default();

    // Build logging config, capturing forwarding info to start later
    let mut log_forwarding: Option<(Receiver<CoreLog>, PyObject)> = None;
    if let Some(logging_conf) = telemetry_config.logging {
        telemetry_build.logging(if let Some(forward_to) = logging_conf.forward_to {
            // Note, actual log forwarding is started later
            let (consumer, stream) = CoreLogStreamConsumer::new(FORWARD_LOG_BUFFER_SIZE);
            log_forwarding = Some((stream, forward_to));
            Logger::Push {
                filter: logging_conf.filter.to_string(),
                consumer: Arc::new(consumer),
            }
        } else {
            Logger::Console {
                filter: logging_conf.filter.to_string(),
            }
        });
    }

    // Build metric config, but actual metrics instance is late-bound after
    // CoreRuntime is created since it needs Tokio runtime
    if let Some(metrics_conf) = telemetry_config.metrics.as_ref() {
        telemetry_build.attach_service_name(metrics_conf.attach_service_name);
        if let Some(prefix) = &metrics_conf.metric_prefix {
            telemetry_build.metric_prefix(prefix.to_string());
        }
    }

    // Create core runtime which starts tokio multi-thread runtime
    let mut core = CoreRuntime::new(
        telemetry_build
            .build()
            .map_err(|err| PyValueError::new_err(format!("Invalid telemetry config: {err}")))?,
        TokioRuntimeBuilder::default(),
    )
    .map_err(|err| PyRuntimeError::new_err(format!("Failed initializing telemetry: {err}")))?;

    // We late-bind the metrics after core runtime is created since it needs
    // the Tokio handle
    let mut metrics_call_buffer: Option<Arc<MetricsCallBuffer<BufferedMetricRef>>> = None;
    if let Some(metrics_conf) = telemetry_config.metrics {
        let _guard = core.tokio_handle().enter();
        // If they want buffered, cannot have Prom/OTel and we make buffered
        if metrics_conf.buffered_with_size > 0 {
            if metrics_conf.opentelemetry.is_some() || metrics_conf.prometheus.is_some() {
                return Err(PyValueError::new_err(
                    "Cannot have buffer size with OpenTelemetry or Prometheus metric config",
                ));
            }
            let buffer = Arc::new(MetricsCallBuffer::new(metrics_conf.buffered_with_size));
            core.telemetry_mut()
                .attach_late_init_metrics(buffer.clone());
            metrics_call_buffer = Some(buffer);
        } else {
            core.telemetry_mut()
                .attach_late_init_metrics(metrics_conf.try_into()?);
        }
    }

    // Start log forwarding if needed
    let log_forwarder_handle = log_forwarding.map(|(stream, callback)| {
        Arc::new(core.tokio_handle().spawn(async move {
            let mut stream = std::pin::pin!(stream.chunks_timeout(
                FORWARD_LOG_BUFFER_SIZE,
                Duration::from_millis(FORWARD_LOG_MAX_FREQ_MS)
            ));
            while let Some(core_logs) = stream.next().await {
                // Create vec of buffered logs
                let entries = core_logs
                    .into_iter()
                    .map(|core_log| BufferedLogEntry { core_log })
                    .collect::<Vec<_>>();
                // We silently swallow errors here because logging them could
                // cause a bad loop and we don't want to assume console presence
                let _ = Python::with_gil(|py| callback.call1(py, (entries,)));
            }
        }))
    });

    Ok(RuntimeRef {
        runtime: Runtime {
            core: Arc::new(core),
            metrics_call_buffer,
            log_forwarder_handle,
        },
    })
}

pub fn raise_in_thread(
    _py: Python,
    thread_id: std::os::raw::c_long,
    exc: &Bound<'_, PyAny>,
) -> bool {
    unsafe { pyo3::ffi::PyThreadState_SetAsyncExc(thread_id, exc.as_ptr()) == 1 }
}

impl Runtime {
    pub fn future_into_py<'a, F, T>(&self, py: Python<'a>, fut: F) -> PyResult<Bound<'a, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'py> IntoPyObject<'py>,
    {
        let _guard = self.core.tokio_handle().enter();
        pyo3_async_runtimes::generic::future_into_py::<TokioRuntime, _, T>(py, fut)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Stop the log forwarder
        if let Some(handle) = self.log_forwarder_handle.as_ref() {
            handle.abort();
        }
    }
}

#[pymethods]
impl RuntimeRef {
    fn retrieve_buffered_metrics(
        &self,
        py: Python,
        durations_as_seconds: bool,
    ) -> Vec<BufferedMetricUpdate> {
        convert_metric_events(
            py,
            self.runtime
                .metrics_call_buffer
                .as_ref()
                .expect("Attempting to retrieve buffered metrics without buffer")
                .retrieve(),
            durations_as_seconds,
        )
    }

    fn write_test_info_log(&self, message: &str, extra_data: &str) {
        let _g = tracing::subscriber::set_default(
            self.runtime
                .core
                .telemetry()
                .trace_subscriber()
                .unwrap()
                .clone(),
        );
        tracing::info!(message, extra_data = extra_data);
    }

    fn write_test_debug_log(&self, message: &str, extra_data: &str) {
        let _g = tracing::subscriber::set_default(
            self.runtime
                .core
                .telemetry()
                .trace_subscriber()
                .unwrap()
                .clone(),
        );
        tracing::debug!(message, extra_data = extra_data);
    }
}

// WARNING: This must match temporalio.bridge.runtime.BufferedLogEntry protocol
#[pymethods]
impl BufferedLogEntry {
    #[getter]
    fn target(&self) -> &str {
        &self.core_log.target
    }

    #[getter]
    fn message(&self) -> &str {
        &self.core_log.message
    }

    #[getter]
    fn time(&self) -> f64 {
        self.core_log
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs_f64()
    }

    #[getter]
    fn level(&self) -> u8 {
        // Convert to Python log levels, with trace as 9
        match self.core_log.level {
            Level::TRACE => 9,
            Level::DEBUG => 10,
            Level::INFO => 20,
            Level::WARN => 30,
            Level::ERROR => 40,
        }
    }

    #[getter]
    fn fields(&self, py: Python<'_>) -> PyResult<HashMap<&str, PyObject>> {
        self.core_log
            .fields
            .iter()
            .map(|(key, value)| match pythonize(py, value) {
                Ok(value) => Ok((key.as_str(), value.unbind())),
                Err(err) => Err(err.into()),
            })
            .collect()
    }
}

impl TryFrom<MetricsConfig> for Arc<dyn CoreMeter> {
    type Error = PyErr;

    fn try_from(conf: MetricsConfig) -> PyResult<Self> {
        if let Some(otel_conf) = conf.opentelemetry {
            if conf.prometheus.is_some() {
                return Err(PyValueError::new_err(
                    "Cannot have OpenTelemetry and Prometheus metrics",
                ));
            }

            // Build OTel exporter
            let mut build = OtelCollectorOptionsBuilder::default();
            build
                .url(
                    Url::parse(&otel_conf.url).map_err(|err| {
                        PyValueError::new_err(format!("Invalid OTel URL: {err}"))
                    })?,
                )
                .headers(otel_conf.headers)
                .use_seconds_for_durations(otel_conf.durations_as_seconds);
            if let Some(period) = otel_conf.metric_periodicity_millis {
                build.metric_periodicity(Duration::from_millis(period));
            }
            if otel_conf.metric_temporality_delta {
                build.metric_temporality(MetricTemporality::Delta);
            }
            if let Some(global_tags) = conf.global_tags {
                build.global_tags(global_tags);
            }
            if otel_conf.http {
                build.protocol(OtlpProtocol::Http);
            }
            let otel_options = build
                .build()
                .map_err(|err| PyValueError::new_err(format!("Invalid OTel config: {err}")))?;
            Ok(Arc::new(build_otlp_metric_exporter(otel_options).map_err(
                |err| PyValueError::new_err(format!("Failed building OTel exporter: {err}")),
            )?))
        } else if let Some(prom_conf) = conf.prometheus {
            // Start prom exporter
            let mut build = PrometheusExporterOptionsBuilder::default();
            build
                .socket_addr(
                    SocketAddr::from_str(&prom_conf.bind_address).map_err(|err| {
                        PyValueError::new_err(format!("Invalid Prometheus address: {err}"))
                    })?,
                )
                .counters_total_suffix(prom_conf.counters_total_suffix)
                .unit_suffix(prom_conf.unit_suffix)
                .use_seconds_for_durations(prom_conf.durations_as_seconds);
            if let Some(global_tags) = conf.global_tags {
                build.global_tags(global_tags);
            }
            if let Some(overrides) = prom_conf.histogram_bucket_overrides {
                build.histogram_bucket_overrides(
                    temporal_sdk_core_api::telemetry::HistogramBucketOverrides { overrides },
                );
            }
            let prom_options = build.build().map_err(|err| {
                PyValueError::new_err(format!("Invalid Prometheus config: {err}"))
            })?;
            Ok(start_prometheus_metric_exporter(prom_options)
                .map_err(|err| {
                    PyValueError::new_err(format!("Failed starting Prometheus exporter: {err}"))
                })?
                .meter)
        } else {
            Err(PyValueError::new_err(
                "Either OpenTelemetry or Prometheus config must be provided",
            ))
        }
    }
}

// Code below through the rest of the file is similar to
// https://github.com/awestlake87/pyo3-asyncio/blob/v0.16.0/src/tokio.rs but
// altered to support spawning based on current Tokio runtime instead of a
// single static one

pub(crate) struct TokioRuntime;

tokio::task_local! {
    static TASK_LOCALS: std::cell::OnceCell<pyo3_async_runtimes::TaskLocals>;
}

impl pyo3_async_runtimes::generic::Runtime for TokioRuntime {
    type JoinError = tokio::task::JoinError;
    type JoinHandle = tokio::task::JoinHandle<()>;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::runtime::Handle::current().spawn(fut)
    }
}

impl pyo3_async_runtimes::generic::ContextExt for TokioRuntime {
    fn scope<F, R>(
        locals: pyo3_async_runtimes::TaskLocals,
        fut: F,
    ) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = std::cell::OnceCell::new();
        cell.set(locals).unwrap();

        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<pyo3_async_runtimes::TaskLocals> {
        TASK_LOCALS
            .try_with(|c| {
                c.get()
                    .map(|locals| Python::with_gil(|py| locals.clone_ref(py)))
            })
            .unwrap_or_default()
    }
}
