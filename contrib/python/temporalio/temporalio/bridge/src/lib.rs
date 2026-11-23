use pyo3::prelude::*;
use pyo3::types::PyTuple;

mod client;
mod client_rpc_generated;
mod envconfig;
mod metric;
mod runtime;
mod testing;
mod worker;

#[pymodule]
fn temporal_sdk_bridge(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Lets ops assert at runtime that the FIPS wheel is actually loaded, e.g.
    // `temporalio.bridge.temporal_sdk_bridge.FIPS is True`. Mirrors sdk-ruby PR #466.
    m.add("FIPS", cfg!(feature = "fips"))?;

    // Under a FIPS build, install aws-lc-rs (FIPS mode) as the process-wide
    // rustls provider before any client is constructed. This makes
    // CryptoProvider::get_default() resolve to aws-lc-rs everywhere (including
    // the custom verifier in client.rs), so no `ring` provider is ever used.
    #[cfg(feature = "fips")]
    {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    // Client stuff
    m.add("RPCError", py.get_type::<client::RPCError>())?;
    m.add_class::<client::ClientRef>()?;
    m.add_function(wrap_pyfunction!(connect_client, m)?)?;

    // Metric stuff
    m.add_class::<metric::MetricMeterRef>()?;
    m.add_class::<metric::MetricAttributesRef>()?;
    m.add_class::<metric::MetricCounterRef>()?;
    m.add_class::<metric::MetricHistogramRef>()?;
    m.add_class::<metric::MetricHistogramFloatRef>()?;
    m.add_class::<metric::MetricHistogramDurationRef>()?;
    m.add_class::<metric::MetricGaugeRef>()?;
    m.add_class::<metric::MetricGaugeFloatRef>()?;
    m.add_class::<metric::BufferedMetricUpdate>()?;
    m.add_class::<metric::BufferedMetric>()?;
    m.add_function(wrap_pyfunction!(new_metric_meter, m)?)?;

    // Runtime stuff
    m.add_class::<runtime::RuntimeRef>()?;
    m.add_class::<runtime::BufferedLogEntry>()?;
    m.add_function(wrap_pyfunction!(init_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(raise_in_thread, m)?)?;

    // Testing stuff
    m.add_class::<testing::EphemeralServerRef>()?;
    m.add_function(wrap_pyfunction!(start_dev_server, m)?)?;
    m.add_function(wrap_pyfunction!(start_test_server, m)?)?;

    // Worker stuff
    m.add(
        "PollShutdownError",
        py.get_type::<worker::PollShutdownError>(),
    )?;
    m.add_class::<worker::WorkerRef>()?;
    m.add_class::<worker::HistoryPusher>()?;
    m.add_class::<worker::CustomSlotSupplier>()?;
    m.add_class::<worker::SlotReserveCtx>()?;
    m.add_class::<worker::SlotReleaseCtx>()?;
    m.add_class::<worker::SlotMarkUsedCtx>()?;
    m.add_class::<worker::WorkflowSlotInfo>()?;
    m.add_class::<worker::ActivitySlotInfo>()?;
    m.add_class::<worker::LocalActivitySlotInfo>()?;
    m.add_function(wrap_pyfunction!(new_worker, m)?)?;
    m.add_function(wrap_pyfunction!(new_replay_worker, m)?)?;

    // envconfig
    let envconfig_module = PyModule::new(py, "envconfig")?;
    envconfig_module.add("ConfigError", py.get_type::<envconfig::ConfigError>())?;
    envconfig_module.add_function(wrap_pyfunction!(envconfig::load_client_config, m)?)?;
    envconfig_module.add_function(wrap_pyfunction!(envconfig::load_client_connect_config, m)?)?;
    m.add_submodule(&envconfig_module)?;

    Ok(())
}

#[pyfunction]
fn connect_client<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: client::ClientConfig,
) -> PyResult<Bound<'a, PyAny>> {
    client::connect_client(py, runtime_ref, config)
}

#[pyfunction]
fn new_metric_meter(runtime_ref: &runtime::RuntimeRef) -> Option<metric::MetricMeterRef> {
    metric::new_metric_meter(runtime_ref)
}

#[pyfunction]
fn init_runtime(options: runtime::RuntimeOptions) -> PyResult<runtime::RuntimeRef> {
    runtime::init_runtime(options)
}

#[pyfunction]
fn raise_in_thread(py: Python, thread_id: std::os::raw::c_long, exc: &Bound<'_, PyAny>) -> bool {
    runtime::raise_in_thread(py, thread_id, exc)
}

#[pyfunction]
fn start_dev_server<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: testing::DevServerConfig,
) -> PyResult<Bound<'a, PyAny>> {
    testing::start_dev_server(py, runtime_ref, config)
}

#[pyfunction]
fn start_test_server<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: testing::TestServerConfig,
) -> PyResult<Bound<'a, PyAny>> {
    testing::start_test_server(py, runtime_ref, config)
}

#[pyfunction]
fn new_worker(
    runtime_ref: &runtime::RuntimeRef,
    client: &client::ClientRef,
    config: worker::WorkerConfig,
) -> PyResult<worker::WorkerRef> {
    worker::new_worker(runtime_ref, client, config)
}

#[pyfunction]
fn new_replay_worker<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: worker::WorkerConfig,
) -> PyResult<Bound<'a, PyTuple>> {
    worker::new_replay_worker(py, runtime_ref, config)
}
