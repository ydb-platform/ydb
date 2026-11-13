use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use temporalio_client::tonic::{
    self,
    metadata::{AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue},
};
use temporalio_client::{
    ClientKeepAliveOptions as CoreClientKeepAliveConfig, Connection, ConnectionOptions,
    DnsLoadBalancingOptions, GrpcCompression, HttpConnectProxyOptions, RetryOptions,
};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::client::WebPkiServerVerifier;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{self, DigitallySignedStruct, RootCertStore, SignatureScheme};
use tracing::warn;
use url::Url;

use crate::runtime;

pyo3::create_exception!(temporal_sdk_bridge, RPCError, PyException);

#[pyclass]
pub struct ClientRef {
    pub(crate) connection: Connection,
    pub(crate) runtime: runtime::Runtime,
}

#[derive(FromPyObject)]
pub struct ClientConfig {
    target_url: String,
    client_name: String,
    client_version: String,
    metadata: HashMap<String, RpcMetadataValue>,
    api_key: Option<String>,
    identity: String,
    tls_config: Option<ClientTlsConfig>,
    retry_config: Option<ClientRetryConfig>,
    keep_alive_config: Option<ClientKeepAliveConfig>,
    http_connect_proxy_config: Option<ClientHttpConnectProxyConfig>,
    dns_load_balancing_config: Option<ClientDnsLoadBalancingConfig>,
    grpc_compression: String,
    payloads_warn_size: u64,
    memo_warn_size: u64,
}

#[derive(FromPyObject)]
struct ClientTlsConfig {
    server_root_ca_cert: Option<Vec<u8>>,
    domain: Option<String>,
    client_cert: Option<Vec<u8>>,
    client_private_key: Option<Vec<u8>>,
    verification_server_name: Option<String>,
}

#[derive(FromPyObject)]
struct ClientRetryConfig {
    pub initial_interval_millis: u64,
    pub randomization_factor: f64,
    pub multiplier: f64,
    pub max_interval_millis: u64,
    pub max_elapsed_time_millis: Option<u64>,
    pub max_retries: usize,
}

#[derive(FromPyObject)]
struct ClientKeepAliveConfig {
    pub interval_millis: u64,
    pub timeout_millis: u64,
}

#[derive(FromPyObject)]
struct ClientHttpConnectProxyConfig {
    pub target_host: String,
    pub basic_auth: Option<(String, String)>,
}

#[derive(FromPyObject)]
struct ClientDnsLoadBalancingConfig {
    pub resolution_interval_millis: u64,
}

#[derive(FromPyObject)]
pub(crate) struct RpcCall {
    pub(crate) rpc: String,
    req: Vec<u8>,
    pub(crate) retry: bool,
    metadata: HashMap<String, RpcMetadataValue>,
    timeout_millis: Option<u64>,
}

#[derive(FromPyObject)]
enum RpcMetadataValue {
    #[pyo3(transparent, annotation = "str")]
    Str(String),
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Vec<u8>),
}

pub fn connect_client<'a>(
    py: Python<'a>,
    runtime_ref: &runtime::RuntimeRef,
    config: ClientConfig,
) -> PyResult<Bound<'a, PyAny>> {
    let metrics_meter = runtime_ref
        .runtime
        .core
        .telemetry()
        .get_temporal_metric_meter();
    let opts = config.into_connection_options(metrics_meter)?;
    runtime_ref.runtime.assert_same_process("create client")?;
    let runtime = runtime_ref.runtime.clone();
    runtime_ref.runtime.future_into_py(py, async move {
        Ok(ClientRef {
            connection: Connection::connect(opts)
                .await
                .map_err(|err| PyRuntimeError::new_err(format!("Failed client connect: {err}")))?,
            runtime,
        })
    })
}

#[macro_export]
macro_rules! rpc_call {
    ($connection:ident, $call:ident, $trait:tt, $service_method:ident, $call_name:ident) => {
        if $call.retry {
            rpc_resp($trait::$call_name(&mut $connection, rpc_req($call)?).await)
        } else {
            rpc_resp(
                $connection
                    .$service_method()
                    .$call_name(rpc_req($call)?)
                    .await,
            )
        }
    };
}

#[pymethods]
impl ClientRef {
    fn update_metadata(&self, headers: HashMap<String, RpcMetadataValue>) -> PyResult<()> {
        let (ascii_headers, binary_headers) = partition_headers(headers);

        self.connection
            .set_headers(ascii_headers)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        self.connection
            .set_binary_headers(binary_headers)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        Ok(())
    }

    fn update_api_key(&self, api_key: Option<String>) {
        self.connection.set_api_key(api_key);
    }
}

pub(crate) fn rpc_req<P: prost::Message + Default>(call: RpcCall) -> PyResult<tonic::Request<P>> {
    let proto = P::decode(&*call.req)
        .map_err(|err| PyValueError::new_err(format!("Invalid proto: {err}")))?;
    let mut req = tonic::Request::new(proto);
    for (k, v) in call.metadata {
        if let Ok(binary_key) = BinaryMetadataKey::from_str(&k) {
            let RpcMetadataValue::Bytes(bytes) = v else {
                return Err(PyValueError::new_err(format!(
                    "Invalid metadata value for binary key {k}: expected bytes"
                )));
            };

            req.metadata_mut()
                .insert_bin(binary_key, BinaryMetadataValue::from_bytes(&bytes));
        } else {
            let ascii_key = AsciiMetadataKey::from_str(&k)
                .map_err(|err| PyValueError::new_err(format!("Invalid metadata key: {err}")))?;

            let RpcMetadataValue::Str(string) = v else {
                return Err(PyValueError::new_err(format!(
                    "Invalid metadata value for ASCII key {k}: expected str"
                )));
            };

            req.metadata_mut().insert(
                ascii_key,
                AsciiMetadataValue::from_str(&string).map_err(|err| {
                    PyValueError::new_err(format!("Invalid metadata value: {err}"))
                })?,
            );
        }
    }
    if let Some(timeout_millis) = call.timeout_millis {
        req.set_timeout(Duration::from_millis(timeout_millis));
    }
    Ok(req)
}

pub(crate) fn rpc_resp<P>(res: Result<tonic::Response<P>, tonic::Status>) -> PyResult<Vec<u8>>
where
    P: prost::Message,
    P: Default,
{
    match res {
        Ok(resp) => Ok(resp.get_ref().encode_to_vec()),
        Err(err) => {
            Python::attach(move |py| {
                // Create tuple of "status", "message", and optional "details"
                let code = err.code() as u32;
                let message = err.message().to_owned();
                let details = err.details().into_pyobject(py)?.unbind();
                Err(RPCError::new_err((code, message, details)))
            })
        }
    }
}

fn partition_headers(
    headers: HashMap<String, RpcMetadataValue>,
) -> (HashMap<String, String>, HashMap<String, Vec<u8>>) {
    let (ascii_enum_headers, binary_enum_headers): (HashMap<_, _>, HashMap<_, _>) = headers
        .into_iter()
        .partition(|(_, v)| matches!(v, RpcMetadataValue::Str(_)));

    let ascii_headers = ascii_enum_headers
        .into_iter()
        .map(|(k, v)| {
            let RpcMetadataValue::Str(s) = v else {
                unreachable!();
            };
            (k, s)
        })
        .collect();
    let binary_headers = binary_enum_headers
        .into_iter()
        .map(|(k, v)| {
            let RpcMetadataValue::Bytes(b) = v else {
                unreachable!();
            };
            (k, b)
        })
        .collect();

    (ascii_headers, binary_headers)
}

impl ClientConfig {
    fn into_connection_options(
        self,
        metrics_meter: Option<temporalio_common::telemetry::metrics::TemporalMeter>,
    ) -> PyResult<ConnectionOptions> {
        let (ascii_headers, binary_headers) = partition_headers(self.metadata);
        let has_proxy = self.http_connect_proxy_config.is_some();
        // Core rejects DNS load balancing alongside an HTTP CONNECT proxy, so
        // suppress DNS LB whenever a proxy is configured to keep the
        // pre-existing behavior even if a caller leaves the default.
        let dns_load_balancing = if has_proxy {
            warn!("Disabling DNS load balancing because http_connect_proxy_config is set");
            None
        } else {
            self.dns_load_balancing_config.map(Into::into)
        };
        let conn_opts = ConnectionOptions::new(
            Url::parse(&self.target_url)
                .map_err(|err| PyValueError::new_err(format!("invalid target URL: {err}")))?,
        )
        .client_name(self.client_name)
        .client_version(self.client_version)
        .identity(self.identity)
        .retry_options(
            self.retry_config
                .map_or(RetryOptions::default(), |c| c.into()),
        )
        .keep_alive(self.keep_alive_config.map(Into::into))
        .maybe_http_connect_proxy(self.http_connect_proxy_config.map(Into::into))
        .dns_load_balancing(dns_load_balancing)
        .grpc_compression(grpc_compression_from_str(&self.grpc_compression)?)
        .payload_limits(
            temporalio_client::PayloadLimitsOptions::builder()
                .payloads_warn_size(self.payloads_warn_size)
                .memo_warn_size(self.memo_warn_size)
                .build(),
        )
        .headers(ascii_headers)
        .binary_headers(binary_headers)
        .maybe_api_key(self.api_key)
        .maybe_tls_options(if let Some(tls_config) = self.tls_config {
            Some(tls_config.try_into()?)
        } else {
            None
        })
        .maybe_metrics_meter(metrics_meter);
        Ok(conn_opts.build())
    }
}

fn grpc_compression_from_str(value: &str) -> PyResult<GrpcCompression> {
    match value {
        "none" => Ok(GrpcCompression::None),
        "gzip" => Ok(GrpcCompression::Gzip),
        _ => Err(PyValueError::new_err(format!(
            "invalid grpc_compression: {value}"
        ))),
    }
}

impl TryFrom<ClientTlsConfig> for temporalio_client::TlsOptions {
    type Error = PyErr;

    fn try_from(conf: ClientTlsConfig) -> PyResult<Self> {
        let mut server_root_ca_cert = conf.server_root_ca_cert;
        let server_cert_verifier = match conf.verification_server_name {
            None => None,
            Some(name) => {
                // The CA bundle is consumed by the verifier's own root store; a
                // custom verifier cannot be combined with roots on the connection.
                let ca_cert = server_root_ca_cert.take().ok_or_else(|| {
                    PyValueError::new_err(
                        "Must have server root CA cert when verification server name is set",
                    )
                })?;
                Some(fixed_server_name_verifier(&name, &ca_cert)?)
            }
        };
        let client_tls_options = match (conf.client_cert, conf.client_private_key) {
            (None, None) => None,
            (Some(client_cert), Some(client_private_key)) => Some(
                temporalio_client::ClientTlsOptions::builder()
                    .client_cert(client_cert)
                    .client_private_key(client_private_key)
                    .build(),
            ),
            _ => {
                return Err(PyValueError::new_err(
                    "Must have both client cert and private key or neither",
                ))
            }
        };
        Ok(temporalio_client::TlsOptions::builder()
            .maybe_server_root_ca_cert(server_root_ca_cert)
            .maybe_domain(conf.domain)
            .maybe_client_tls_options(client_tls_options)
            .maybe_server_cert_verifier(server_cert_verifier)
            .build())
    }
}

/// Builds a standard WebPKI verifier over the given root CA bundle that
/// checks the certificate against `verification_server_name` rather than the
/// connection's server name, leaving SNI/`:authority` to follow the
/// connected host (or `domain` when set).
fn fixed_server_name_verifier(
    verification_server_name: &str,
    ca_cert_pem: &[u8],
) -> PyResult<Arc<dyn ServerCertVerifier>> {
    let certs = CertificateDer::pem_slice_iter(ca_cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            PyValueError::new_err(format!("Invalid server root CA cert PEM: {err:?}"))
        })?;
    // Root loading and provider selection mirror tonic's default (no custom
    // verifier) client path: unparsable certificates in the bundle are
    // skipped, and the provider is the process default if one is installed,
    // else the build's compiled-in provider. Under a `fips` build lib.rs
    // installs aws-lc-rs (FIPS) as the process default, so `get_default()`
    // returns it and this fallback is not taken; the fallback is still made
    // `cfg`-conditional because the `ring` module is absent in a FIPS build
    // (`tokio-rustls/ring` off) and would otherwise fail to compile.
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(certs);
    let provider = CryptoProvider::get_default().cloned().unwrap_or_else(|| {
        #[cfg(feature = "fips")]
        {
            Arc::new(rustls::crypto::aws_lc_rs::default_provider())
        }
        #[cfg(not(feature = "fips"))]
        {
            Arc::new(rustls::crypto::ring::default_provider())
        }
    });
    let inner = WebPkiServerVerifier::builder_with_provider(roots.into(), provider)
        .build()
        .map_err(|err| {
            PyValueError::new_err(format!("Failed building certificate verifier: {err}"))
        })?;
    let server_name = ServerName::try_from(verification_server_name.to_owned())
        .map_err(|err| PyValueError::new_err(format!("Invalid verification server name: {err}")))?;
    Ok(Arc::new(FixedServerNameVerifier { inner, server_name }))
}

/// Delegates to the standard WebPKI verifier, but verifies the certificate
/// against a fixed server name instead of the connection's server name.
#[derive(Debug)]
struct FixedServerNameVerifier {
    inner: Arc<WebPkiServerVerifier>,
    server_name: ServerName<'static>,
}

impl ServerCertVerifier for FixedServerNameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.inner.verify_server_cert(
            end_entity,
            intermediates,
            &self.server_name,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

impl From<ClientRetryConfig> for RetryOptions {
    fn from(conf: ClientRetryConfig) -> Self {
        RetryOptions::builder()
            .initial_interval(Duration::from_millis(conf.initial_interval_millis))
            .randomization_factor(conf.randomization_factor)
            .multiplier(conf.multiplier)
            .max_interval(Duration::from_millis(conf.max_interval_millis))
            .max_elapsed_time(conf.max_elapsed_time_millis.map(Duration::from_millis))
            .max_retries(conf.max_retries)
            .build()
    }
}

impl From<ClientKeepAliveConfig> for CoreClientKeepAliveConfig {
    fn from(conf: ClientKeepAliveConfig) -> Self {
        CoreClientKeepAliveConfig::builder()
            .interval(Duration::from_millis(conf.interval_millis))
            .timeout(Duration::from_millis(conf.timeout_millis))
            .build()
    }
}

impl From<ClientHttpConnectProxyConfig> for HttpConnectProxyOptions {
    fn from(conf: ClientHttpConnectProxyConfig) -> Self {
        HttpConnectProxyOptions::new(conf.target_host)
            .maybe_basic_auth(conf.basic_auth)
            .build()
    }
}

impl From<ClientDnsLoadBalancingConfig> for DnsLoadBalancingOptions {
    fn from(conf: ClientDnsLoadBalancingConfig) -> Self {
        let mut opts = DnsLoadBalancingOptions::default();
        opts.resolution_interval = Duration::from_millis(conf.resolution_interval_millis);
        opts
    }
}
