use std::time::Duration;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use temporal_sdk_core::ephemeral_server;

use crate::runtime;

#[pyclass]
pub struct EphemeralServerRef {
    server: Option<ephemeral_server::EphemeralServer>,
    runtime: runtime::Runtime,
}

#[derive(FromPyObject)]
pub struct DevServerConfig {
    existing_path: Option<String>,
    sdk_name: String,
    sdk_version: String,
    download_version: String,
    download_dest_dir: Option<String>,
    download_ttl_ms: Option<u64>,
    namespace: String,
    ip: String,
    port: Option<u16>,
    database_filename: Option<String>,
    ui: bool,
    log_format: String,
    log_level: String,
    extra_args: Vec<String>,
}

#[derive(FromPyObject)]
pub struct TestServerConfig {
    existing_path: Option<String>,
    sdk_name: String,
    sdk_version: String,
    download_version: String,
    download_dest_dir: Option<String>,
    download_ttl_ms: Option<u64>,
    port: Option<u16>,
    extra_args: Vec<String>,
}

pub fn start_dev_server<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: DevServerConfig,
) -> PyResult<Bound<'a, PyAny>> {
    let opts: ephemeral_server::TemporalDevServerConfig = config.try_into()?;
    let runtime = runtime_ref.runtime.clone();
    runtime_ref.runtime.future_into_py(py, async move {
        Ok(EphemeralServerRef {
            server: Some(opts.start_server().await.map_err(|err| {
                PyRuntimeError::new_err(format!("Failed starting Temporal dev server: {err}"))
            })?),
            runtime,
        })
    })
}

pub fn start_test_server<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: TestServerConfig,
) -> PyResult<Bound<'a, PyAny>> {
    let opts: ephemeral_server::TestServerConfig = config.try_into()?;
    let runtime = runtime_ref.runtime.clone();
    runtime_ref.runtime.future_into_py(py, async move {
        Ok(EphemeralServerRef {
            server: Some(opts.start_server().await.map_err(|err| {
                PyRuntimeError::new_err(format!("Failed starting test server: {err}"))
            })?),
            runtime,
        })
    })
}

#[pymethods]
impl EphemeralServerRef {
    #[getter]
    fn target(&self) -> PyResult<String> {
        let server = self
            .server
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Server shutdown"))?;
        Ok(server.target.clone())
    }

    #[getter]
    fn has_test_service(&self) -> PyResult<bool> {
        let server = self
            .server
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("Server shutdown"))?;
        Ok(server.has_test_service)
    }

    fn shutdown<'p>(&mut self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let server = self.server.take();
        self.runtime.future_into_py(py, async move {
            if let Some(mut server) = server {
                server.shutdown().await.map_err(|err| {
                    PyRuntimeError::new_err(format!("Failed shutting down Temporalite: {err}"))
                })?;
            }
            Ok(())
        })
    }
}

impl TryFrom<DevServerConfig> for ephemeral_server::TemporalDevServerConfig {
    type Error = PyErr;

    fn try_from(conf: DevServerConfig) -> PyResult<Self> {
        ephemeral_server::TemporalDevServerConfigBuilder::default()
            .exe(if let Some(existing_path) = conf.existing_path {
                ephemeral_server::EphemeralExe::ExistingPath(existing_path.to_owned())
            } else {
                ephemeral_server::EphemeralExe::CachedDownload {
                    version: if conf.download_version != "default" {
                        ephemeral_server::EphemeralExeVersion::Fixed(conf.download_version)
                    } else {
                        ephemeral_server::EphemeralExeVersion::SDKDefault {
                            sdk_name: conf.sdk_name,
                            sdk_version: conf.sdk_version,
                        }
                    },
                    dest_dir: conf.download_dest_dir,
                    ttl: conf.download_ttl_ms.map(Duration::from_millis),
                }
            })
            .namespace(conf.namespace)
            .ip(conf.ip)
            .port(conf.port)
            .db_filename(conf.database_filename)
            .ui(conf.ui)
            .log((conf.log_format, conf.log_level))
            .extra_args(conf.extra_args)
            .build()
            .map_err(|err| PyValueError::new_err(format!("Invalid Temporalite config: {err}")))
    }
}

impl TryFrom<TestServerConfig> for ephemeral_server::TestServerConfig {
    type Error = PyErr;

    fn try_from(conf: TestServerConfig) -> PyResult<Self> {
        ephemeral_server::TestServerConfigBuilder::default()
            .exe(if let Some(existing_path) = conf.existing_path {
                ephemeral_server::EphemeralExe::ExistingPath(existing_path.to_owned())
            } else {
                ephemeral_server::EphemeralExe::CachedDownload {
                    version: if conf.download_version != "default" {
                        ephemeral_server::EphemeralExeVersion::Fixed(conf.download_version)
                    } else {
                        ephemeral_server::EphemeralExeVersion::SDKDefault {
                            sdk_name: conf.sdk_name,
                            sdk_version: conf.sdk_version,
                        }
                    },
                    dest_dir: conf.download_dest_dir,
                    ttl: conf.download_ttl_ms.map(Duration::from_millis),
                }
            })
            .port(conf.port)
            .extra_args(conf.extra_args)
            .build()
            .map_err(|err| PyValueError::new_err(format!("Invalid test server config: {err}")))
    }
}
