use std::env;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

use http::{HeaderMap, HeaderName, HeaderValue};
use opentelemetry::otel_debug;
use tonic::codec::CompressionEncoding;
use tonic::metadata::{KeyAndValueRef, MetadataMap};
use tonic::service::Interceptor;
use tonic::transport::Channel;
#[cfg(any(
    feature = "tls",
    feature = "tls-ring",
    feature = "tls-aws-lc",
    feature = "tls-provider-agnostic"
))]
use tonic::transport::ClientTlsConfig;

use super::{default_headers, parse_header_string, OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DEFAULT};
use super::{resolve_timeout, ExporterBuildError};
use crate::exporter::Compression;
use crate::{exporter::ExportConfig, OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_HEADERS};

#[cfg(all(
    feature = "experimental-grpc-retry",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
use crate::retry::retry_with_backoff;
#[cfg(feature = "grpc-tonic")]
use crate::retry::RetryPolicy;
#[cfg(all(
    feature = "experimental-grpc-retry",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
use opentelemetry_sdk::runtime::Runtime;
#[cfg(all(
    feature = "grpc-tonic",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
use std::future::Future;

#[cfg(feature = "logs")]
pub(crate) mod logs;

#[cfg(feature = "metrics")]
pub(crate) mod metrics;

#[cfg(feature = "trace")]
pub(crate) mod trace;

/// Configuration for [tonic]
///
/// [tonic]: https://github.com/hyperium/tonic
#[derive(Debug, Default)]
#[non_exhaustive]
pub(crate) struct TonicConfig {
    /// Custom metadata entries to send to the collector.
    pub(crate) metadata: Option<MetadataMap>,
    /// TLS settings for the collector endpoint.
    #[cfg(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    ))]
    pub(crate) tls_config: Option<ClientTlsConfig>,
    /// The compression algorithm to use when communicating with the collector.
    pub(crate) compression: Option<Compression>,
    pub(crate) channel: Option<tonic::transport::Channel>,
    pub(crate) interceptor: Option<BoxInterceptor>,
    /// The retry policy to use for gRPC requests.
    #[cfg(feature = "experimental-grpc-retry")]
    pub(crate) retry_policy: Option<RetryPolicy>,
}

impl TryFrom<Compression> for tonic::codec::CompressionEncoding {
    type Error = ExporterBuildError;

    fn try_from(value: Compression) -> Result<Self, ExporterBuildError> {
        match value {
            #[cfg(feature = "gzip-tonic")]
            Compression::Gzip => Ok(tonic::codec::CompressionEncoding::Gzip),
            #[cfg(not(feature = "gzip-tonic"))]
            Compression::Gzip => Err(ExporterBuildError::FeatureRequiredForCompressionAlgorithm(
                "gzip-tonic",
                Compression::Gzip,
            )),
            #[cfg(feature = "zstd-tonic")]
            Compression::Zstd => Ok(tonic::codec::CompressionEncoding::Zstd),
            #[cfg(not(feature = "zstd-tonic"))]
            Compression::Zstd => Err(ExporterBuildError::FeatureRequiredForCompressionAlgorithm(
                "zstd-tonic",
                Compression::Zstd,
            )),
        }
    }
}

/// Configuration for the [tonic] OTLP GRPC exporter.
///
/// It allows you to
/// - add additional metadata
/// - set tls config (via the `tls-ring`, `tls-aws-lc`, or `tls-provider-agnostic` features)
/// - specify custom [channel]s
///
/// [tonic]: <https://github.com/hyperium/tonic>
/// [channel]: tonic::transport::Channel
///
/// ## Examples
///
/// ```no_run
/// # #[cfg(feature="metrics")]
/// use opentelemetry_sdk::metrics::Temporality;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a span exporter you can use to when configuring tracer providers
/// # #[cfg(feature="trace")]
/// let span_exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic().build()?;
///
/// // Create a metric exporter you can use when configuring meter providers
/// # #[cfg(feature="metrics")]
/// let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
///     .with_tonic()
///     .with_temporality(Temporality::default())
///     .build()?;
///
/// // Create a log exporter you can use when configuring logger providers
/// # #[cfg(feature="logs")]
/// let log_exporter = opentelemetry_otlp::LogExporter::builder().with_tonic().build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct TonicExporterBuilder {
    pub(crate) tonic_config: TonicConfig,
    pub(crate) exporter_config: ExportConfig,
}

pub(crate) struct BoxInterceptor(Box<dyn Interceptor + Send + Sync>);
impl tonic::service::Interceptor for BoxInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        self.0.call(request)
    }
}

impl Debug for BoxInterceptor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BoxInterceptor(..)")
    }
}

impl Default for TonicExporterBuilder {
    fn default() -> Self {
        TonicExporterBuilder {
            tonic_config: TonicConfig {
                metadata: Some(MetadataMap::from_headers(
                    (&default_headers())
                        .try_into()
                        .expect("Invalid tonic headers"),
                )),
                #[cfg(any(
                    feature = "tls",
                    feature = "tls-ring",
                    feature = "tls-aws-lc",
                    feature = "tls-provider-agnostic"
                ))]
                tls_config: None,
                compression: None,
                channel: Option::default(),
                interceptor: Option::default(),
                #[cfg(feature = "experimental-grpc-retry")]
                retry_policy: None,
            },
            exporter_config: ExportConfig {
                protocol: Some(crate::Protocol::Grpc),
                ..Default::default()
            },
        }
    }
}

impl TonicExporterBuilder {
    // This is for clippy to work with only the grpc-tonic feature enabled
    #[allow(unused)]
    fn build_channel(
        self,
        signal_endpoint_var: &str,
        signal_timeout_var: &str,
        signal_compression_var: &str,
        signal_headers_var: &str,
        signal_protocol_var: &str,
    ) -> Result<
        (
            Channel,
            BoxInterceptor,
            Option<CompressionEncoding>,
            Option<RetryPolicy>,
        ),
        ExporterBuildError,
    > {
        // Resolve protocol and validate compatibility with gRPC transport.
        // Note: TonicExporterBuilder defaults protocol to Some(Grpc), so for the
        // typical `.with_tonic().build()` path, resolve_protocol returns Grpc
        // immediately (env vars are skipped because programmatic config takes
        // precedence). This validation primarily catches programmatic misuse like
        // `.with_tonic().with_protocol(Protocol::HttpBinary).build()`.
        // Env var-based transport selection is handled at the auto-select layer
        // (e.g., SpanExporter::builder().build()), which routes to the correct
        // transport before reaching this code.
        #[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
        {
            let protocol =
                super::resolve_protocol(signal_protocol_var, self.exporter_config.protocol);

            let is_http_protocol = false;
            #[cfg(feature = "http-proto")]
            let is_http_protocol =
                is_http_protocol || matches!(protocol, crate::Protocol::HttpBinary);
            #[cfg(feature = "http-json")]
            let is_http_protocol =
                is_http_protocol || matches!(protocol, crate::Protocol::HttpJson);
            if is_http_protocol {
                return Err(ExporterBuildError::InvalidConfig {
                    name: "protocol".to_string(),
                    reason:
                        "HTTP protocol is not compatible with gRPC transport. Use `.with_http()` instead."
                            .to_string(),
                });
            }
        }

        let compression = self.resolve_compression(signal_compression_var)?;

        let (headers_from_env, headers_for_logging) = parse_headers_from_env(signal_headers_var);

        let metadata = merge_metadata_with_headers_from_env(
            self.tonic_config.metadata.unwrap_or_default(),
            headers_from_env,
        );

        let add_metadata = move |mut req: tonic::Request<()>| {
            for key_and_value in metadata.iter() {
                match key_and_value {
                    KeyAndValueRef::Ascii(key, value) => {
                        req.metadata_mut().append(key, value.to_owned())
                    }
                    KeyAndValueRef::Binary(key, value) => {
                        req.metadata_mut().append_bin(key, value.to_owned())
                    }
                };
            }

            Ok(req)
        };

        let interceptor = match self.tonic_config.interceptor {
            Some(mut interceptor) => {
                BoxInterceptor(Box::new(move |req| interceptor.call(add_metadata(req)?)))
            }
            None => BoxInterceptor(Box::new(add_metadata)),
        };

        // Get retry policy before consuming self
        #[cfg(feature = "experimental-grpc-retry")]
        let retry_policy = self.tonic_config.retry_policy.clone();

        // If a custom channel was provided, use that channel instead of creating one
        if let Some(channel) = self.tonic_config.channel {
            return Ok((
                channel,
                interceptor,
                compression,
                #[cfg(feature = "experimental-grpc-retry")]
                retry_policy,
                #[cfg(not(feature = "experimental-grpc-retry"))]
                None,
            ));
        }

        let config = self.exporter_config;

        let endpoint = Self::resolve_endpoint(signal_endpoint_var, config.endpoint);

        // Used for logging the endpoint
        let endpoint_clone = endpoint.clone();

        let endpoint = tonic::transport::Endpoint::from_shared(endpoint)
            .map_err(|op| ExporterBuildError::InvalidUri(endpoint_clone.clone(), op.to_string()))?;

        let is_https = endpoint
            .uri()
            .scheme()
            .is_some_and(|s| *s == http::uri::Scheme::HTTPS);

        #[cfg(not(any(
            feature = "tls",
            feature = "tls-ring",
            feature = "tls-aws-lc",
            feature = "tls-provider-agnostic"
        )))]
        if is_https {
            return Err(ExporterBuildError::InvalidConfig {
                name: "endpoint".to_string(),
                reason: format!(
                    "endpoint '{}' uses HTTPS but no TLS feature is enabled; \
                     enable one of the `tls-ring`, `tls-aws-lc`, or `tls-provider-agnostic` features on `opentelemetry-otlp`",
                    endpoint_clone
                ),
            });
        }
        let timeout = resolve_timeout(signal_timeout_var, config.timeout.as_ref());

        #[cfg(any(
            feature = "tls",
            feature = "tls-ring",
            feature = "tls-aws-lc",
            feature = "tls-provider-agnostic"
        ))]
        let channel = match self.tonic_config.tls_config {
            Some(tls_config) => endpoint
                .tls_config(tls_config)
                .map_err(|er| ExporterBuildError::InternalFailure(er.to_string()))?,
            None if is_https => endpoint
                .tls_config(ClientTlsConfig::new())
                .map_err(|er| ExporterBuildError::InternalFailure(er.to_string()))?,
            None => endpoint,
        }
        .timeout(timeout)
        .connect_lazy();

        #[cfg(not(any(
            feature = "tls",
            feature = "tls-ring",
            feature = "tls-aws-lc",
            feature = "tls-provider-agnostic"
        )))]
        let channel = endpoint.timeout(timeout).connect_lazy();

        otel_debug!(name: "TonicChannelBuilt", endpoint = endpoint_clone, timeout_in_millisecs = timeout.as_millis(), compression = format!("{:?}", compression), headers = format!("{:?}", headers_for_logging));
        Ok((
            channel,
            interceptor,
            compression,
            #[cfg(feature = "experimental-grpc-retry")]
            retry_policy,
            #[cfg(not(feature = "experimental-grpc-retry"))]
            None,
        ))
    }

    fn resolve_endpoint(default_endpoint_var: &str, provided_endpoint: Option<String>) -> String {
        // resolving endpoint string
        // grpc doesn't have a "path" like http(See https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md)
        // the path of grpc calls are based on the protobuf service definition
        // so we won't append one for default grpc endpoints
        // If users for some reason want to use a custom path, they can use env var or builder to pass it
        //
        // programmatic configuration overrides any value set via environment variables
        if let Some(endpoint) = provided_endpoint.filter(|s| !s.is_empty()) {
            endpoint
        } else if let Ok(endpoint) = env::var(default_endpoint_var) {
            endpoint
        } else if let Ok(endpoint) = env::var(OTEL_EXPORTER_OTLP_ENDPOINT) {
            endpoint
        } else {
            OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DEFAULT.to_string()
        }
    }

    fn resolve_compression(
        &self,
        env_override: &str,
    ) -> Result<Option<CompressionEncoding>, ExporterBuildError> {
        super::resolve_compression_from_env(self.tonic_config.compression, env_override)?
            .map(|c| c.try_into())
            .transpose()
    }

    /// Build a new tonic log exporter
    #[cfg(feature = "logs")]
    pub(crate) fn build_log_exporter(self) -> Result<crate::logs::LogExporter, ExporterBuildError> {
        use crate::exporter::tonic::logs::TonicLogsClient;

        otel_debug!(name: "LogsTonicChannelBuilding");

        let (channel, interceptor, compression, retry_policy) = self.build_channel(
            crate::logs::OTEL_EXPORTER_OTLP_LOGS_ENDPOINT,
            crate::logs::OTEL_EXPORTER_OTLP_LOGS_TIMEOUT,
            crate::logs::OTEL_EXPORTER_OTLP_LOGS_COMPRESSION,
            crate::logs::OTEL_EXPORTER_OTLP_LOGS_HEADERS,
            crate::logs::OTEL_EXPORTER_OTLP_LOGS_PROTOCOL,
        )?;

        let client = TonicLogsClient::new(channel, interceptor, compression, retry_policy);

        Ok(crate::logs::LogExporter::from_tonic(client))
    }

    /// Build a new tonic metrics exporter
    #[cfg(feature = "metrics")]
    pub(crate) fn build_metrics_exporter(
        self,
        temporality: opentelemetry_sdk::metrics::Temporality,
    ) -> Result<crate::MetricExporter, ExporterBuildError> {
        use crate::MetricExporter;
        use metrics::TonicMetricsClient;

        otel_debug!(name: "MetricsTonicChannelBuilding");

        let (channel, interceptor, compression, retry_policy) = self.build_channel(
            crate::metric::OTEL_EXPORTER_OTLP_METRICS_ENDPOINT,
            crate::metric::OTEL_EXPORTER_OTLP_METRICS_TIMEOUT,
            crate::metric::OTEL_EXPORTER_OTLP_METRICS_COMPRESSION,
            crate::metric::OTEL_EXPORTER_OTLP_METRICS_HEADERS,
            crate::metric::OTEL_EXPORTER_OTLP_METRICS_PROTOCOL,
        )?;

        let client = TonicMetricsClient::new(channel, interceptor, compression, retry_policy);

        Ok(MetricExporter::from_tonic(client, temporality))
    }

    /// Build a new tonic span exporter
    #[cfg(feature = "trace")]
    pub(crate) fn build_span_exporter(self) -> Result<crate::SpanExporter, ExporterBuildError> {
        use crate::exporter::tonic::trace::TonicTracesClient;

        otel_debug!(name: "TracesTonicChannelBuilding");

        let (channel, interceptor, compression, retry_policy) = self.build_channel(
            crate::span::OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
            crate::span::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
            crate::span::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION,
            crate::span::OTEL_EXPORTER_OTLP_TRACES_HEADERS,
            crate::span::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL,
        )?;

        let client = TonicTracesClient::new(channel, interceptor, compression, retry_policy);

        Ok(crate::SpanExporter::from_tonic(client))
    }
}

/// Wrapper for retry functionality in tonic exporters.
/// Provides a unified call path that either uses retry_with_backoff when experimental-grpc-retry
/// feature is enabled, or executes the operation once when it's not.
#[cfg(all(
    feature = "grpc-tonic",
    feature = "experimental-grpc-retry",
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
async fn tonic_retry_with_backoff<R, F, Fut, T>(
    runtime: R,
    policy: RetryPolicy,
    classify_fn: fn(&tonic::Status) -> crate::retry::RetryErrorType,
    operation_name: &'static str,
    operation: F,
) -> Result<T, tonic::Status>
where
    R: Runtime,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    retry_with_backoff(runtime, policy, classify_fn, operation_name, operation).await
}

/// Provides a unified call path when experimental-grpc-retry is not enabled - just executes the operation once.
#[cfg(all(
    feature = "grpc-tonic",
    not(feature = "experimental-grpc-retry"),
    any(feature = "trace", feature = "metrics", feature = "logs")
))]
async fn tonic_retry_with_backoff<F, Fut, T>(
    _runtime: (),
    _policy: RetryPolicy,
    _classify_fn: fn(&tonic::Status) -> crate::retry::RetryErrorType,
    _operation_name: &'static str,
    operation: F,
) -> Result<T, tonic::Status>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, tonic::Status>>,
{
    operation().await
}

#[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
/// Log and convert a `tonic::Status` from a failed export into an `OTelSdkError`.
///
/// The gRPC code, message, and details are logged at DEBUG level only, since
/// the message may contain sensitive information such as authentication tokens
/// echoed back by the server.
///
/// The returned `OTelSdkError` never contains the gRPC message, only the code.
/// When `status.source()` is `Some`, the status was locally generated by
/// tonic's transport stack (connect failure, invalid URL, DNS, etc.); its
/// source chain is library-deterministic text and is appended to the returned
/// error so misconfigurations surface at ERROR via the SDK processors.
/// We don't log at WARN here because SDK processors (BatchLogProcessor,
/// BatchSpanProcessor, PeriodicReader) already log the returned error via
/// `otel_error!`.
///
/// `$client_name` must be a string literal so that `concat!` can produce
/// compile-time event names consistent with the codebase naming convention.
macro_rules! handle_tonic_export_error {
    ($client_name:literal, $tonic_status:expr) => {{
        let status = &$tonic_status;
        let code = status.code();
        otel_debug!(
            name: concat!($client_name, ".ExportFailed"),
            grpc_code = format!("{:?}", code),
            grpc_message = status.message(),
            grpc_details = format!("{:?}", status.details())
        );
        let mut err_msg = format!(
            concat!($client_name, " export failed with gRPC code: {:?}"),
            code
        );
        if let Some(src) = std::error::Error::source(status) {
            err_msg.push_str(": ");
            err_msg.push_str(&$crate::exporter::tonic::render_source_chain(src));
        }
        Err(opentelemetry_sdk::error::OTelSdkError::InternalFailure(
            err_msg,
        ))
    }};
}

#[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
/// Log and convert a `tonic::Status` from a failed interceptor into a new
/// `tonic::Status` suitable for retry classification.
///
/// Interceptor errors are always treated as potentially sensitive since
/// interceptors are the primary mechanism for adding auth tokens. Only the
/// gRPC code is included in the returned status message; the original message
/// and details are logged at DEBUG level only.
macro_rules! handle_interceptor_error {
    ($client_name:literal, $e:expr) => {{
        let status = &$e;
        otel_debug!(
            name: concat!($client_name, ".InterceptorFailed"),
            grpc_code = format!("{:?}", status.code()),
            grpc_message = status.message(),
            grpc_details = format!("{:?}", status.details())
        );
        tonic::Status::internal(format!(
            concat!(
                $client_name,
                " export failed in interceptor with gRPC code: {:?}"
            ),
            status.code()
        ))
    }};
}

// Make macros available to submodules (logs, trace, metrics).
#[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
pub(crate) use handle_interceptor_error;
#[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
pub(crate) use handle_tonic_export_error;

/// Render an `std::error::Error` and its `source()` chain into a single
/// colon-separated string (e.g. `"transport error: invalid URL, scheme is missing"`).
#[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
pub(crate) fn render_source_chain(err: &(dyn std::error::Error + 'static)) -> String {
    use std::fmt::Write;
    let mut out = err.to_string();
    let mut next = err.source();
    while let Some(cur) = next {
        let _ = write!(&mut out, ": {cur}");
        next = cur.source();
    }
    out
}

fn merge_metadata_with_headers_from_env(
    metadata: MetadataMap,
    headers_from_env: HeaderMap,
) -> MetadataMap {
    if headers_from_env.is_empty() {
        metadata
    } else {
        let mut existing_headers: HeaderMap = metadata.into_headers();
        existing_headers.extend(headers_from_env);

        MetadataMap::from_headers(existing_headers)
    }
}

fn parse_headers_from_env(signal_headers_var: &str) -> (HeaderMap, Vec<String>) {
    let mut headers = Vec::new();

    (
        env::var(signal_headers_var)
            .or_else(|_| env::var(OTEL_EXPORTER_OTLP_HEADERS))
            .map(|input| {
                parse_header_string(&input)
                    .filter_map(|(key, value)| {
                        headers.push(key.to_owned());
                        Some((
                            HeaderName::from_str(key).ok()?,
                            HeaderValue::from_str(&value).ok()?,
                        ))
                    })
                    .collect::<HeaderMap>()
            })
            .unwrap_or_default(),
        headers,
    )
}

/// Expose interface for modifying [TonicConfig] fields within the exporter builders.
pub(crate) trait HasTonicConfig {
    /// Return a mutable reference to the export config within the exporter builders.
    fn tonic_config(&mut self) -> &mut TonicConfig;
}

/// Expose interface for modifying [TonicConfig] fields within the [TonicExporterBuilder].
impl HasTonicConfig for TonicExporterBuilder {
    fn tonic_config(&mut self) -> &mut TonicConfig {
        &mut self.tonic_config
    }
}

/// Expose methods to override tonic-specific configuration.
///
/// ## Examples
/// ```
/// # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
/// # {
/// use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
/// let exporter_builder = opentelemetry_otlp::SpanExporter::builder()
///     .with_tonic()
///     .with_compression(opentelemetry_otlp::Compression::Gzip);
/// # }
/// ```
pub trait WithTonicConfig {
    /// Set the TLS settings for the collector endpoint.
    #[cfg(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    ))]
    fn with_tls_config(self, tls_config: ClientTlsConfig) -> Self;

    /// Set custom metadata entries to send to the collector.
    ///
    /// **Note**: This method is additive - calling it multiple times will merge
    /// the metadata entries. If the same key is provided in multiple calls,
    /// the last value will override previous ones.
    ///
    /// # Example
    /// ```no_run
    /// # #[cfg(feature = "grpc-tonic")]
    /// # {
    /// use tonic::metadata::MetadataMap;
    /// use opentelemetry_otlp::WithTonicConfig;
    ///
    /// let mut metadata1 = MetadataMap::new();
    /// metadata1.insert("key1", "value1".parse().unwrap());
    ///
    /// let mut metadata2 = MetadataMap::new();
    /// metadata2.insert("key2", "value2".parse().unwrap());
    ///
    /// let exporter = opentelemetry_otlp::SpanExporter::builder()
    ///     .with_tonic()
    ///     .with_metadata(metadata1)  // Adds key1=value1
    ///     .with_metadata(metadata2)  // Adds key2=value2 (both are present)
    ///     .build()?;
    /// # }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn with_metadata(self, metadata: MetadataMap) -> Self;

    /// Set the compression algorithm to use when communicating with the collector.
    fn with_compression(self, compression: Compression) -> Self;

    /// Use `channel` as tonic's transport channel.
    /// this will override tls config and should only be used
    /// when working with non-HTTP transports.
    ///
    /// Users MUST make sure the timeout is
    /// the same as the channel's timeout.
    fn with_channel(self, channel: tonic::transport::Channel) -> Self;

    /// Use a custom `interceptor` to modify each outbound request.
    /// This can be used to modify the gRPC metadata, for example
    /// to inject auth tokens.
    ///
    /// **Note**: Calling this method multiple times will replace the previous
    /// interceptor. If you need multiple interceptors, chain them together
    /// before passing to this method.
    ///
    /// # Examples
    ///
    /// ## Single interceptor
    /// ```no_run
    /// # #[cfg(feature = "grpc-tonic")]
    /// # {
    /// use tonic::{Request, Status};
    /// use opentelemetry_otlp::WithTonicConfig;
    ///
    /// fn auth_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    ///     req.metadata_mut().insert("authorization", "Bearer token".parse().unwrap());
    ///     Ok(req)
    /// }
    ///
    /// let exporter = opentelemetry_otlp::SpanExporter::builder()
    ///     .with_tonic()
    ///     .with_interceptor(auth_interceptor)
    ///     .build()?;
    /// # }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// ## Multiple interceptors (chaining)
    /// ```no_run
    /// # #[cfg(feature = "grpc-tonic")]
    /// # {
    /// use tonic::{Request, Status};
    /// use opentelemetry_otlp::WithTonicConfig;
    ///
    /// fn auth_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    ///     req.metadata_mut().insert("authorization", "Bearer token".parse().unwrap());
    ///     Ok(req)
    /// }
    ///
    /// fn logging_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    ///     println!("Sending gRPC request with metadata: {:?}", req.metadata());
    ///     Ok(req)
    /// }
    ///
    /// // Chain interceptors by wrapping them
    /// fn combined_interceptor(req: Request<()>) -> Result<Request<()>, Status> {
    ///     let req = logging_interceptor(req)?;
    ///     auth_interceptor(req)
    /// }
    ///
    /// let exporter = opentelemetry_otlp::SpanExporter::builder()
    ///     .with_tonic()
    ///     .with_interceptor(combined_interceptor)
    ///     .build()?;
    /// # }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn with_interceptor<I>(self, interceptor: I) -> Self
    where
        I: tonic::service::Interceptor + Clone + Send + Sync + 'static;

    /// Set the retry policy for gRPC requests.
    #[cfg(feature = "experimental-grpc-retry")]
    fn with_retry_policy(self, policy: RetryPolicy) -> Self;
}

impl<B: HasTonicConfig> WithTonicConfig for B {
    #[cfg(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    ))]
    fn with_tls_config(mut self, tls_config: ClientTlsConfig) -> Self {
        self.tonic_config().tls_config = Some(tls_config);
        self
    }

    /// Set custom metadata entries to send to the collector.
    fn with_metadata(mut self, metadata: MetadataMap) -> Self {
        // extending metadata maps is harder than just casting back/forth
        let mut existing_headers = self
            .tonic_config()
            .metadata
            .clone()
            .unwrap_or_default()
            .into_headers();
        existing_headers.extend(metadata.into_headers());

        self.tonic_config().metadata = Some(MetadataMap::from_headers(existing_headers));
        self
    }

    fn with_compression(mut self, compression: Compression) -> Self {
        self.tonic_config().compression = Some(compression);
        self
    }

    fn with_channel(mut self, channel: tonic::transport::Channel) -> Self {
        self.tonic_config().channel = Some(channel);
        self
    }

    fn with_interceptor<I>(mut self, interceptor: I) -> Self
    where
        I: tonic::service::Interceptor + Clone + Send + Sync + 'static,
    {
        self.tonic_config().interceptor = Some(BoxInterceptor(Box::new(interceptor)));
        self
    }

    #[cfg(feature = "experimental-grpc-retry")]
    fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.tonic_config().retry_policy = Some(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::exporter::tests::run_env_test;
    use crate::exporter::tonic::WithTonicConfig;
    #[cfg(feature = "grpc-tonic")]
    use crate::exporter::Compression;
    use crate::{TonicExporterBuilder, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT};
    use crate::{OTEL_EXPORTER_OTLP_HEADERS, OTEL_EXPORTER_OTLP_TRACES_HEADERS};
    use http::{HeaderMap, HeaderName, HeaderValue};
    use tonic::metadata::{MetadataMap, MetadataValue};

    #[test]
    fn test_with_metadata() {
        // metadata should merge with the current one with priority instead of just replacing it
        let mut metadata = MetadataMap::new();
        metadata.insert("foo", "bar".parse().unwrap());
        let builder = TonicExporterBuilder::default().with_metadata(metadata);
        let result = builder.tonic_config.metadata.unwrap();
        let foo = result
            .get("foo")
            .expect("there to always be an entry for foo");
        assert_eq!(foo, &MetadataValue::try_from("bar").unwrap());
        assert!(result.get("User-Agent").is_some());

        // metadata should override entries with the same key in the default one
        let mut metadata = MetadataMap::new();
        metadata.insert("user-agent", "baz".parse().unwrap());
        let builder = TonicExporterBuilder::default().with_metadata(metadata);
        let result = builder.tonic_config.metadata.unwrap();
        assert_eq!(
            result.get("User-Agent").unwrap(),
            &MetadataValue::try_from("baz").unwrap()
        );
        assert_eq!(
            result.len(),
            TonicExporterBuilder::default()
                .tonic_config
                .metadata
                .unwrap()
                .len()
        );
    }

    #[test]
    #[cfg(feature = "gzip-tonic")]
    fn test_with_gzip_compression() {
        let builder = TonicExporterBuilder::default().with_compression(Compression::Gzip);
        assert_eq!(builder.tonic_config.compression.unwrap(), Compression::Gzip);
    }

    #[test]
    #[cfg(feature = "zstd-tonic")]
    fn test_with_zstd_compression() {
        let builder = TonicExporterBuilder::default().with_compression(Compression::Zstd);
        assert_eq!(builder.tonic_config.compression.unwrap(), Compression::Zstd);
    }

    #[test]
    fn test_convert_compression() {
        #[cfg(feature = "gzip-tonic")]
        assert!(tonic::codec::CompressionEncoding::try_from(Compression::Gzip).is_ok());
        #[cfg(not(feature = "gzip-tonic"))]
        assert!(tonic::codec::CompressionEncoding::try_from(Compression::Gzip).is_err());
        #[cfg(feature = "zstd-tonic")]
        assert!(tonic::codec::CompressionEncoding::try_from(Compression::Zstd).is_ok());
        #[cfg(not(feature = "zstd-tonic"))]
        assert!(tonic::codec::CompressionEncoding::try_from(Compression::Zstd).is_err());
    }

    #[cfg(feature = "zstd-tonic")]
    #[test]
    fn test_priority_of_signal_env_over_generic_env_for_compression() {
        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION, "zstd"),
                (crate::OTEL_EXPORTER_OTLP_COMPRESSION, "gzip"),
            ],
            || {
                let builder = TonicExporterBuilder::default();

                let compression = builder
                    .resolve_compression(crate::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION)
                    .unwrap();
                assert_eq!(compression, Some(tonic::codec::CompressionEncoding::Zstd));
            },
        );
    }

    #[cfg(feature = "zstd-tonic")]
    #[test]
    fn test_priority_of_code_based_config_over_envs_for_compression() {
        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION, "gzip"),
                (crate::OTEL_EXPORTER_OTLP_COMPRESSION, "gzip"),
            ],
            || {
                let builder = TonicExporterBuilder::default().with_compression(Compression::Zstd);

                let compression = builder
                    .resolve_compression(crate::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION)
                    .unwrap();
                assert_eq!(compression, Some(tonic::codec::CompressionEncoding::Zstd));
            },
        );
    }

    #[test]
    fn test_use_default_when_others_missing_for_compression() {
        run_env_test(vec![], || {
            let builder = TonicExporterBuilder::default();

            let compression = builder
                .resolve_compression(crate::OTEL_EXPORTER_OTLP_TRACES_COMPRESSION)
                .unwrap();
            assert!(compression.is_none());
        });
    }

    #[test]
    fn test_parse_headers_from_env() {
        run_env_test(
            vec![
                (OTEL_EXPORTER_OTLP_TRACES_HEADERS, "k1=v1,k2=v2"),
                (OTEL_EXPORTER_OTLP_HEADERS, "k3=v3"),
            ],
            || {
                assert_eq!(
                    super::parse_headers_from_env(OTEL_EXPORTER_OTLP_TRACES_HEADERS).0,
                    HeaderMap::from_iter([
                        (
                            HeaderName::from_static("k1"),
                            HeaderValue::from_static("v1")
                        ),
                        (
                            HeaderName::from_static("k2"),
                            HeaderValue::from_static("v2")
                        ),
                    ])
                );

                assert_eq!(
                    super::parse_headers_from_env("EMPTY_ENV").0,
                    HeaderMap::from_iter([(
                        HeaderName::from_static("k3"),
                        HeaderValue::from_static("v3")
                    )])
                );
            },
        )
    }

    #[test]
    fn test_merge_metadata_with_headers_from_env() {
        run_env_test(
            vec![(OTEL_EXPORTER_OTLP_TRACES_HEADERS, "k1=v1,k2=v2")],
            || {
                let headers_from_env =
                    super::parse_headers_from_env(OTEL_EXPORTER_OTLP_TRACES_HEADERS);

                let mut metadata = MetadataMap::new();
                metadata.insert("foo", "bar".parse().unwrap());
                metadata.insert("k1", "v0".parse().unwrap());

                let result =
                    super::merge_metadata_with_headers_from_env(metadata, headers_from_env.0);

                assert_eq!(
                    result.get("foo").unwrap(),
                    MetadataValue::from_static("bar")
                );
                assert_eq!(result.get("k1").unwrap(), MetadataValue::from_static("v1"));
                assert_eq!(result.get("k2").unwrap(), MetadataValue::from_static("v2"));
            },
        );
    }

    #[test]
    fn test_priority_of_signal_env_over_generic_env_for_endpoint() {
        run_env_test(
            vec![
                (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "http://localhost:1234"),
                (super::OTEL_EXPORTER_OTLP_ENDPOINT, "http://localhost:2345"),
            ],
            || {
                let url = TonicExporterBuilder::resolve_endpoint(
                    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
                    None,
                );
                assert_eq!(url, "http://localhost:1234");
            },
        );
    }

    #[test]
    fn test_priority_of_code_based_config_over_envs_for_endpoint() {
        run_env_test(
            vec![
                (OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, "http://localhost:1234"),
                (super::OTEL_EXPORTER_OTLP_ENDPOINT, "http://localhost:2345"),
            ],
            || {
                let url = TonicExporterBuilder::resolve_endpoint(
                    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
                    Some("http://localhost:3456".to_string()),
                );
                assert_eq!(url, "http://localhost:3456");
            },
        );
    }

    #[test]
    fn test_use_default_when_others_missing_for_endpoint() {
        run_env_test(vec![], || {
            let url =
                TonicExporterBuilder::resolve_endpoint(OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, None);
            assert_eq!(url, "http://localhost:4317");
        });
    }

    #[test]
    fn test_use_default_when_empty_string_for_option() {
        run_env_test(vec![], || {
            let url = TonicExporterBuilder::resolve_endpoint(
                OTEL_EXPORTER_OTLP_TRACES_ENDPOINT,
                Some(String::new()),
            );
            assert_eq!(url, "http://localhost:4317");
        });
    }

    #[cfg(feature = "experimental-grpc-retry")]
    #[test]
    fn test_with_retry_policy() {
        use crate::retry::RetryPolicy;
        use crate::WithTonicConfig;

        let custom_policy = RetryPolicy {
            max_retries: 5,
            initial_delay_ms: 200,
            max_delay_ms: 3200,
            jitter_ms: 50,
        };

        let builder = TonicExporterBuilder::default().with_retry_policy(custom_policy);

        // Verify the retry policy was set
        let retry_policy = builder.tonic_config.retry_policy.as_ref().unwrap();
        assert_eq!(retry_policy.max_retries, 5);
        assert_eq!(retry_policy.initial_delay_ms, 200);
        assert_eq!(retry_policy.max_delay_ms, 3200);
        assert_eq!(retry_policy.jitter_ms, 50);
    }

    #[cfg(feature = "experimental-grpc-retry")]
    #[test]
    fn test_default_retry_policy_when_none_configured() {
        // This test requires us to create a tonic client, but we can't easily do that without
        // a channel in a unit test. The default behavior is tested implicitly in integration tests.
        let builder = TonicExporterBuilder::default();
        assert!(builder.tonic_config.retry_policy.is_none());
    }

    #[test]
    #[cfg(not(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    )))]
    fn test_https_endpoint_errors_without_tls_feature() {
        use crate::exporter::ExporterBuildError;
        use crate::SpanExporter;
        use crate::WithExportConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_endpoint("https://example.com")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ExporterBuildError::InvalidConfig { .. }),
            "expected InvalidConfig error, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("HTTPS") && msg.contains("TLS"),
            "error message should mention HTTPS and TLS, got: {msg}"
        );
    }

    #[tokio::test]
    #[cfg(any(
        feature = "tls",
        feature = "tls-ring",
        feature = "tls-aws-lc",
        feature = "tls-provider-agnostic"
    ))]
    async fn test_https_endpoint_succeeds_with_tls_feature() {
        use crate::SpanExporter;
        use crate::WithExportConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_endpoint("https://example.com")
            .build();

        assert!(
            result.is_ok(),
            "https endpoint should succeed when TLS feature is enabled, got: {:?}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_http_endpoint_succeeds_without_tls_feature() {
        use crate::SpanExporter;
        use crate::WithExportConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_endpoint("http://localhost:4317")
            .build();

        assert!(
            result.is_ok(),
            "http endpoint should always succeed, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    #[cfg(not(feature = "gzip-tonic"))]
    fn test_gzip_compression_errors_without_feature() {
        use crate::exporter::ExporterBuildError;
        use crate::SpanExporter;
        use crate::WithTonicConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_compression(crate::Compression::Gzip)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                ExporterBuildError::FeatureRequiredForCompressionAlgorithm(..)
            ),
            "expected FeatureRequiredForCompressionAlgorithm error, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("gzip-tonic"),
            "error message should mention 'gzip-tonic' feature, got: {msg}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "gzip-tonic")]
    async fn test_gzip_compression_succeeds_with_feature() {
        use crate::SpanExporter;
        use crate::WithTonicConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_compression(crate::Compression::Gzip)
            .build();

        assert!(
            result.is_ok(),
            "gzip compression should succeed when gzip-tonic feature is enabled, got: {:?}",
            result.unwrap_err()
        );
    }

    #[test]
    #[cfg(not(feature = "zstd-tonic"))]
    fn test_zstd_compression_errors_without_feature() {
        use crate::exporter::ExporterBuildError;
        use crate::SpanExporter;
        use crate::WithTonicConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_compression(crate::Compression::Zstd)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                ExporterBuildError::FeatureRequiredForCompressionAlgorithm(..)
            ),
            "expected FeatureRequiredForCompressionAlgorithm error, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("zstd-tonic"),
            "error message should mention 'zstd-tonic' feature, got: {msg}"
        );
    }

    #[tokio::test]
    #[cfg(feature = "zstd-tonic")]
    async fn test_zstd_compression_succeeds_with_feature() {
        use crate::SpanExporter;
        use crate::WithTonicConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_compression(crate::Compression::Zstd)
            .build();

        assert!(
            result.is_ok(),
            "zstd compression should succeed when zstd-tonic feature is enabled, got: {:?}",
            result.unwrap_err()
        );
    }

    #[tokio::test]
    async fn test_unix_socket_endpoint_succeeds() {
        use crate::SpanExporter;
        use crate::WithExportConfig;

        let result = SpanExporter::builder()
            .with_tonic()
            .with_endpoint("unix:///tmp/test")
            .build();

        assert!(
            result.is_ok(),
            "unix socket endpoint should succeed, got: {:?}",
            result.unwrap_err()
        );
    }

    #[cfg(any(feature = "trace", feature = "metrics", feature = "logs"))]
    mod error_handling_tests {
        use opentelemetry::otel_debug;
        use opentelemetry_sdk::error::OTelSdkError;

        #[test]
        fn export_error_includes_grpc_code_but_not_sensitive_message() {
            let status = tonic::Status::unauthenticated("Bearer secret-token-123");
            let result: Result<(), OTelSdkError> =
                super::super::handle_tonic_export_error!("TestExporter", status);
            let msg = format!("{}", result.unwrap_err());

            assert!(
                msg.contains("Unauthenticated"),
                "Error should contain the gRPC code, got: {msg}"
            );
            assert!(
                !msg.contains("secret-token-123"),
                "Error must not contain the sensitive token, got: {msg}"
            );
        }

        #[test]
        fn export_error_includes_exporter_name() {
            let status = tonic::Status::unavailable("connection refused");
            let result: Result<(), OTelSdkError> =
                super::super::handle_tonic_export_error!("TonicLogsClient", status);
            let msg = format!("{}", result.unwrap_err());

            assert!(
                msg.contains("TonicLogsClient"),
                "Error should identify the exporter, got: {msg}"
            );
        }

        #[test]
        fn export_error_never_includes_grpc_message() {
            // Neither connection nor sensitive codes should leak the message
            // into the returned error (messages are only logged, not returned)
            let statuses = [
                tonic::Status::unavailable("safe connection info"),
                tonic::Status::unknown("safe connection info"),
                tonic::Status::deadline_exceeded("safe connection info"),
                tonic::Status::resource_exhausted("safe connection info"),
                tonic::Status::aborted("safe connection info"),
                tonic::Status::cancelled("safe connection info"),
                tonic::Status::unauthenticated("Bearer my-secret-token"),
                tonic::Status::permission_denied("Bearer my-secret-token"),
                tonic::Status::internal("Bearer my-secret-token"),
            ];
            for status in &statuses {
                let result: Result<(), OTelSdkError> =
                    super::super::handle_tonic_export_error!("TestExporter", status);
                let msg = format!("{}", result.unwrap_err());
                assert!(
                    msg.contains("TestExporter export failed with gRPC code"),
                    "Expected structured error message, got: {msg}"
                );
                assert!(
                    !msg.contains("safe connection info") && !msg.contains("my-secret-token"),
                    "Error message should not include the gRPC message, got: {msg}"
                );
            }
        }

        #[test]
        fn interceptor_error_returns_internal_status_without_sensitive_data() {
            let original = tonic::Status::unauthenticated("Bearer secret");
            let result = super::super::handle_interceptor_error!("TestExporter", original);

            assert_eq!(result.code(), tonic::Code::Internal);
            assert!(
                result.message().contains("Unauthenticated"),
                "Interceptor error should contain original gRPC code, got: {}",
                result.message()
            );
            assert!(
                !result.message().contains("secret"),
                "Interceptor error must not leak sensitive data, got: {}",
                result.message()
            );
        }

        #[test]
        fn interceptor_error_includes_exporter_name() {
            let original = tonic::Status::internal("some error");
            let result = super::super::handle_interceptor_error!("TonicTracesClient", original);

            assert!(
                result.message().contains("TonicTracesClient"),
                "Interceptor error should identify the exporter, got: {}",
                result.message()
            );
        }

        #[test]
        fn export_error_surfaces_transport_source_chain() {
            // `Status::from_error` is how tonic wraps local transport failures
            // (connect errors, URL parse errors, DNS). These sources are
            // library-deterministic text and safe to surface at ERROR level.
            let transport_err = std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid URL, scheme is missing",
            );
            let status = tonic::Status::from_error(Box::new(transport_err));
            let result: Result<(), OTelSdkError> =
                super::super::handle_tonic_export_error!("TonicTracesClient", status);
            let msg = format!("{}", result.unwrap_err());

            assert!(
                msg.contains("TonicTracesClient export failed with gRPC code"),
                "Expected structured error message, got: {msg}"
            );
            assert!(
                msg.contains("invalid URL, scheme is missing"),
                "Pre-flight transport error text should be surfaced, got: {msg}"
            );
        }

        #[test]
        fn server_returned_status_does_not_surface_message_even_when_similar_to_transport() {
            // Server-returned statuses have no `source()`. Even if the server's
            // message resembles a transport error, it must not leak.
            let status = tonic::Status::unknown("invalid URL, scheme is missing");
            let result: Result<(), OTelSdkError> =
                super::super::handle_tonic_export_error!("TestExporter", status);
            let msg = format!("{}", result.unwrap_err());

            assert!(
                !msg.contains("invalid URL, scheme is missing"),
                "Server-returned status message must not leak, got: {msg}"
            );
        }

        #[test]
        fn render_source_chain_joins_nested_sources() {
            #[derive(Debug)]
            struct Wrap {
                msg: &'static str,
                src: Box<dyn std::error::Error + Send + Sync + 'static>,
            }
            impl std::fmt::Display for Wrap {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(self.msg)
                }
            }
            impl std::error::Error for Wrap {
                fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                    Some(self.src.as_ref())
                }
            }

            let inner = std::io::Error::other("invalid URL");
            let middle = Wrap {
                msg: "connect error",
                src: Box::new(inner),
            };
            let outer = Wrap {
                msg: "transport error",
                src: Box::new(middle),
            };

            let rendered = super::super::render_source_chain(&outer);
            assert_eq!(rendered, "transport error: connect error: invalid URL");
        }
    }
}
