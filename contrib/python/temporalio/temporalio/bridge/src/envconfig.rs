use pyo3::prelude::*;
use pyo3::{
    exceptions::PyRuntimeError,
    types::{PyBytes, PyDict},
};
use std::collections::HashMap;
use temporal_sdk_core_api::envconfig::{
    load_client_config as core_load_client_config,
    load_client_config_profile as core_load_client_config_profile,
    ClientConfig as CoreClientConfig, ClientConfigCodec, ClientConfigProfile as CoreClientConfigProfile,
    ClientConfigTLS as CoreClientConfigTLS, DataSource, LoadClientConfigOptions,
    LoadClientConfigProfileOptions,
};

pyo3::create_exception!(temporal_sdk_bridge, ConfigError, PyRuntimeError);

fn data_source_to_dict(py: Python, ds: &DataSource) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match ds {
        DataSource::Path(p) => dict.set_item("path", p)?,
        DataSource::Data(d) => dict.set_item("data", PyBytes::new(py, d))?,
    };
    Ok(dict.into())
}

fn tls_to_dict(py: Python, tls: &CoreClientConfigTLS) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("disabled", tls.disabled)?;
    if let Some(v) = &tls.client_cert {
        dict.set_item("client_cert", data_source_to_dict(py, v)?)?;
    }
    if let Some(v) = &tls.client_key {
        dict.set_item("client_key", data_source_to_dict(py, v)?)?;
    }
    if let Some(v) = &tls.server_ca_cert {
        dict.set_item("server_ca_cert", data_source_to_dict(py, v)?)?;
    }
    if let Some(v) = &tls.server_name {
        dict.set_item("server_name", v)?;
    }
    dict.set_item("disable_host_verification", tls.disable_host_verification)?;
    Ok(dict.into())
}

fn codec_to_dict(py: Python, codec: &ClientConfigCodec) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(v) = &codec.endpoint {
        dict.set_item("endpoint", v)?;
    }
    if let Some(v) = &codec.auth {
        dict.set_item("auth", v)?;
    }
    Ok(dict.into())
}

fn profile_to_dict(py: Python, profile: &CoreClientConfigProfile) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(v) = &profile.address {
        dict.set_item("address", v)?;
    }
    if let Some(v) = &profile.namespace {
        dict.set_item("namespace", v)?;
    }
    if let Some(v) = &profile.api_key {
        dict.set_item("api_key", v)?;
    }
    if let Some(tls) = &profile.tls {
        dict.set_item("tls", tls_to_dict(py, tls)?)?;
    }
    if let Some(codec) = &profile.codec {
        dict.set_item("codec", codec_to_dict(py, codec)?)?;
    }
    if !profile.grpc_meta.is_empty() {
        dict.set_item("grpc_meta", &profile.grpc_meta)?;
    }
    Ok(dict.into())
}

fn core_config_to_dict(py: Python, core_config: &CoreClientConfig) -> PyResult<PyObject> {
    let profiles_dict = PyDict::new(py);
    for (name, profile) in &core_config.profiles {
        let connect_dict = profile_to_dict(py, profile)?;
        profiles_dict.set_item(name, connect_dict)?;
    }
    Ok(profiles_dict.into())
}

fn load_client_config_inner(
    py: Python,
    config_source: Option<DataSource>,
    config_file_strict: bool,
    disable_file: bool,
    env_vars: Option<HashMap<String, String>>,
) -> PyResult<PyObject> {
    let core_config = if disable_file {
        CoreClientConfig::default()
    } else {
        let options = LoadClientConfigOptions {
            config_source,
            config_file_strict,
        };
        core_load_client_config(options, env_vars.as_ref())
            .map_err(|e| ConfigError::new_err(format!("{e}")))?
    };

    core_config_to_dict(py, &core_config)
}

fn load_client_connect_config_inner(
    py: Python,
    config_source: Option<DataSource>,
    profile: Option<String>,
    disable_file: bool,
    disable_env: bool,
    config_file_strict: bool,
    env_vars: Option<HashMap<String, String>>,
) -> PyResult<PyObject> {
    let options = LoadClientConfigProfileOptions {
        config_source,
        config_file_profile: profile,
        config_file_strict,
        disable_file,
        disable_env,
    };

    let profile = core_load_client_config_profile(options, env_vars.as_ref())
        .map_err(|e| ConfigError::new_err(format!("{e}")))?;

    profile_to_dict(py, &profile)
}

#[pyfunction]
#[pyo3(signature = (path, data, disable_file, config_file_strict, env_vars = None))]
pub fn load_client_config(
    py: Python,
    path: Option<String>,
    data: Option<Vec<u8>>,
    disable_file: bool,
    config_file_strict: bool,
    env_vars: Option<HashMap<String, String>>,
) -> PyResult<PyObject> {
    let config_source = match (path, data) {
        (Some(p), None) => Some(DataSource::Path(p)),
        (None, Some(d)) => Some(DataSource::Data(d)),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(ConfigError::new_err(
                "Cannot specify both path and data for config source",
            ))
        }
    };
    load_client_config_inner(
        py,
        config_source,
        config_file_strict,
        disable_file,
        env_vars,
    )
}

#[pyfunction]
#[pyo3(signature = (profile, path, data, disable_file, disable_env, config_file_strict, env_vars = None))]
#[allow(clippy::too_many_arguments)]
pub fn load_client_connect_config(
    py: Python,
    profile: Option<String>,
    path: Option<String>,
    data: Option<Vec<u8>>,
    disable_file: bool,
    disable_env: bool,
    config_file_strict: bool,
    env_vars: Option<HashMap<String, String>>,
) -> PyResult<PyObject> {
    let config_source = match (path, data) {
        (Some(p), None) => Some(DataSource::Path(p)),
        (None, Some(d)) => Some(DataSource::Data(d)),
        (None, None) => None,
        (Some(_), Some(_)) => {
            return Err(ConfigError::new_err(
                "Cannot specify both path and data for config source",
            ))
        }
    };
    load_client_connect_config_inner(
        py,
        config_source,
        profile,
        disable_file,
        disable_env,
        config_file_strict,
        env_vars,
    )
}