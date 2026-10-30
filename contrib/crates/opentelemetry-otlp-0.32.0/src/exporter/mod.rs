//! OTLP exporter builder and configurations.
//!
//! OTLP supports sending data via different protocols and formats.

#[cfg(any(feature = "http-proto", feature = "http-json"))]
use crate::exporter::http::HttpExporterBuilder;
#[cfg(feature = "grpc-tonic")]
use crate::exporter::tonic::TonicExporterBuilder;
use crate::Protocol;
#[cfg(feature = "serialize")]
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

/// Target to which the exporter is going to send signals, defaults to https://localhost:4317.
/// Learn about the relationship between this constant and metrics/spans/logs at
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md#endpoint-urls-for-otlphttp>
pub const OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
/// Default target to which the exporter is going to send signals.
pub const OTEL_EXPORTER_OTLP_ENDPOINT_DEFAULT: &str = OTEL_EXPORTER_OTLP_HTTP_ENDPOINT_DEFAULT;
/// Key-value pairs to be used as headers associated with gRPC or HTTP requests
/// Example: `k1=v1,k2=v2`
/// Note: as of now, this is only supported for HTTP requests.
pub const OTEL_EXPORTER_OTLP_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";
/// Protocol the exporter will use. Either `http/protobuf` or `grpc`.
pub const OTEL_EXPORTER_OTLP_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_PROTOCOL";
/// Compression algorithm to use, defaults to none.
pub const OTEL_EXPORTER_OTLP_COMPRESSION: &str = "OTEL_EXPORTER_OTLP_COMPRESSION";

/// Protocol value for HTTP with protobuf encoding
pub const OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_PROTOBUF: &str = "http/protobuf";
/// Protocol value for gRPC
pub const OTEL_EXPORTER_OTLP_PROTOCOL_GRPC: &str = "grpc";
/// Protocol value for HTTP with JSON encoding
pub const OTEL_EXPORTER_OTLP_PROTOCOL_HTTP_JSON: &str = "http/json";

/// Max waiting time for the backend to process each signal batch, defaults to 10 seconds.
pub const OTEL_EXPORTER_OTLP_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_TIMEOUT";
/// Default max waiting time for the backend to process each signal batch.
pub const OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT: Duration = Duration::from_millis(10000);

// Endpoints per protocol https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
#[cfg(feature = "grpc-tonic")]
const OTEL_EXPORTER_OTLP_GRPC_ENDPOINT_DEFAULT: &str = "http://localhost:4317";
const OTEL_EXPORTER_OTLP_HTTP_ENDPOINT_DEFAULT: &str = "http://localhost:4318";

#[cfg(any(feature = "http-proto", feature = "http-json"))]
pub(crate) mod http;
#[cfg(feature = "grpc-tonic")]
pub(crate) mod tonic;

/// Configuration for the OTLP exporter.
#[derive(Debug, Default)]
pub(crate) struct ExportConfig {
    /// The address of the OTLP collector.
    /// Default address will be used based on the protocol.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub endpoint: Option<String>,

    /// The protocol to use when communicating with the collector.
    /// `None` means the protocol will be resolved from environment variables
    /// or feature defaults at build time.
    pub protocol: Option<Protocol>,

    /// The timeout to the collector.
    /// The default value is 10 seconds.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    pub timeout: Option<Duration>,
}

/// Resolve protocol with priority:
/// 1. Programmatic configuration (provided value)
/// 2. Signal-specific environment variable
/// 3. Generic OTEL_EXPORTER_OTLP_PROTOCOL environment variable
/// 4. Feature-based default
#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
pub(crate) fn resolve_protocol(
    signal_protocol_var: &str,
    provided_protocol: Option<Protocol>,
) -> Protocol {
    if let Some(protocol) = provided_protocol {
        return protocol;
    }
    if let Some(protocol) = Protocol::parse_from_env_var(signal_protocol_var) {
        return protocol;
    }
    if let Some(protocol) = Protocol::from_env() {
        return protocol;
    }
    Protocol::feature_default()
}

#[derive(Error, Debug)]
/// Errors that can occur while building an exporter.
// TODO: Refine and polish this.
// Non-exhaustive to allow for future expansion without breaking changes.
// This could be refined after polishing and finalizing the errors.
#[non_exhaustive]
pub enum ExporterBuildError {
    /// Spawning a new thread failed.
    #[error("Spawning a new thread failed. Unable to create Reqwest-Blocking client.")]
    ThreadSpawnFailed,

    /// Feature required to use the specified compression algorithm.
    #[cfg(any(not(feature = "gzip-tonic"), not(feature = "zstd-tonic")))]
    #[error("feature '{0}' is required to use the compression algorithm '{1}'")]
    FeatureRequiredForCompressionAlgorithm(&'static str, Compression),

    /// No Http client specified.
    #[error("no http client specified")]
    NoHttpClient,

    /// Unsupported compression algorithm.
    #[error("unsupported compression algorithm '{0}'")]
    UnsupportedCompressionAlgorithm(String),

    /// Invalid URI.
    #[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
    #[error("invalid URI {0}. Reason {1}")]
    InvalidUri(String, String),

    /// Invalid configuration.
    #[error("{name}: {reason}")]
    InvalidConfig {
        /// The configuration name.
        name: String,
        /// The reason the configuration is invalid.
        reason: String,
    },

    /// Failed due to an internal error.
    /// The error message is intended for logging purposes only and should not
    /// be used to make programmatic decisions. It is implementation-specific
    /// and subject to change without notice. Consumers of this error should not
    /// rely on its content beyond logging.
    #[error("Reason: {0}")]
    InternalFailure(String),
}

/// The compression algorithm to use when sending data.
#[cfg_attr(feature = "serialize", derive(Deserialize, Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    /// Compresses data using gzip.
    Gzip,
    /// Compresses data using zstd.
    Zstd,
}

impl Display for Compression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::Gzip => write!(f, "gzip"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}

impl FromStr for Compression {
    type Err = ExporterBuildError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "gzip" => Ok(Compression::Gzip),
            "zstd" => Ok(Compression::Zstd),
            _ => Err(ExporterBuildError::UnsupportedCompressionAlgorithm(
                s.to_string(),
            )),
        }
    }
}

/// Resolve compression from environment variables with priority:
/// 1. Provided config value
/// 2. Signal-specific environment variable
/// 3. Generic OTEL_EXPORTER_OTLP_COMPRESSION
/// 4. None (default)
#[cfg(any(feature = "http-proto", feature = "http-json", feature = "grpc-tonic"))]
fn resolve_compression_from_env(
    config_compression: Option<Compression>,
    signal_env_var: &str,
) -> Result<Option<Compression>, ExporterBuildError> {
    if let Some(compression) = config_compression {
        Ok(Some(compression))
    } else if let Ok(compression) = std::env::var(signal_env_var) {
        Ok(Some(compression.parse::<Compression>()?))
    } else if let Ok(compression) = std::env::var(OTEL_EXPORTER_OTLP_COMPRESSION) {
        Ok(Some(compression.parse::<Compression>()?))
    } else {
        Ok(None)
    }
}

/// Returns the default protocol based on enabled features.
///
/// Note: This does not consult environment variables. Protocol resolution
/// from environment variables is handled internally by the exporter builders.
///
/// Priority order (first available wins):
/// 1. http-json (if enabled)
/// 2. http-proto (if enabled)
/// 3. grpc-tonic (if enabled)
#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
impl Default for Protocol {
    fn default() -> Self {
        Protocol::feature_default()
    }
}

/// default user-agent headers
#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
fn default_headers() -> std::collections::HashMap<String, String> {
    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "User-Agent".to_string(),
        format!("OTel-OTLP-Exporter-Rust/{}", env!("CARGO_PKG_VERSION")),
    );
    headers
}

/// Provide access to the [ExportConfig] field within the exporter builders.
pub(crate) trait HasExportConfig {
    /// Return a mutable reference to the [ExportConfig] within the exporter builders.
    fn export_config(&mut self) -> &mut ExportConfig;
}

/// Provide [ExportConfig] access to the [TonicExporterBuilder].
#[cfg(feature = "grpc-tonic")]
impl HasExportConfig for TonicExporterBuilder {
    fn export_config(&mut self) -> &mut ExportConfig {
        &mut self.exporter_config
    }
}

/// Provide [ExportConfig] access to the [HttpExporterBuilder].
#[cfg(any(feature = "http-proto", feature = "http-json"))]
impl HasExportConfig for HttpExporterBuilder {
    fn export_config(&mut self) -> &mut ExportConfig {
        &mut self.exporter_config
    }
}

/// Expose methods to set export configuration on exporter builders.
///
/// ## Examples
/// ```
/// # #[cfg(all(feature = "trace", feature = "grpc-tonic"))]
/// # {
/// use crate::opentelemetry_otlp::WithExportConfig;
/// let exporter_builder = opentelemetry_otlp::SpanExporter::builder()
///     .with_tonic()
///     .with_endpoint("http://localhost:7201");
/// # }
/// ```
pub trait WithExportConfig {
    /// Set the address of the OTLP collector. If not set or set to empty string, the default address is used.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    fn with_endpoint<T: Into<String>>(self, endpoint: T) -> Self;
    /// Set the protocol to use when communicating with the collector.
    ///
    /// Note that protocols that are not supported by exporters will be ignored. The exporter
    /// will use default protocol in this case.
    ///
    /// ## Note
    /// All exporters in this crate only support one protocol, thus choosing the protocol is a no-op at the moment.
    fn with_protocol(self, protocol: Protocol) -> Self;
    /// Set the timeout to the collector.
    ///
    /// Note: Programmatically setting this will override any value set via the environment variable.
    fn with_timeout(self, timeout: Duration) -> Self;
}

impl<B: HasExportConfig> WithExportConfig for B {
    fn with_endpoint<T: Into<String>>(mut self, endpoint: T) -> Self {
        self.export_config().endpoint = Some(endpoint.into());
        self
    }

    fn with_protocol(mut self, protocol: Protocol) -> Self {
        self.export_config().protocol = Some(protocol);
        self
    }

    fn with_timeout(mut self, timeout: Duration) -> Self {
        self.export_config().timeout = Some(timeout);
        self
    }
}

#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
fn resolve_timeout(signal_timeout_var: &str, provided_timeout: Option<&Duration>) -> Duration {
    // programmatic configuration overrides any value set via environment variables
    if let Some(timeout) = provided_timeout {
        *timeout
    } else if let Some(timeout) = std::env::var(signal_timeout_var)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        // per signal env var is not modified
        Duration::from_millis(timeout)
    } else if let Some(timeout) = std::env::var(OTEL_EXPORTER_OTLP_TIMEOUT)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        // if signal env var is not set, then we check if the OTEL_EXPORTER_OTLP_TIMEOUT env var is set
        Duration::from_millis(timeout)
    } else {
        OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT
    }
}

#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
fn parse_header_string(value: &str) -> impl Iterator<Item = (&str, String)> {
    value
        .split_terminator(',')
        .map(str::trim)
        .filter_map(parse_header_key_value_string)
}

#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
fn url_decode(value: &str) -> Option<String> {
    let mut result = String::with_capacity(value.len());
    let mut chars_to_decode = Vec::<u8>::new();
    let mut all_chars = value.chars();

    loop {
        let ch = all_chars.next();

        if ch.is_some() && ch.unwrap() == '%' {
            chars_to_decode.push(
                u8::from_str_radix(&format!("{}{}", all_chars.next()?, all_chars.next()?), 16)
                    .ok()?,
            );
            continue;
        }

        if !chars_to_decode.is_empty() {
            result.push_str(std::str::from_utf8(&chars_to_decode).ok()?);
            chars_to_decode.clear();
        }

        if let Some(c) = ch {
            result.push(c);
        } else {
            return Some(result);
        }
    }
}

#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
fn parse_header_key_value_string(key_value_string: &str) -> Option<(&str, String)> {
    key_value_string
        .split_once('=')
        .map(|(key, value)| {
            (
                key.trim(),
                url_decode(value.trim()).unwrap_or(value.to_string()),
            )
        })
        .filter(|(key, value)| !key.is_empty() && !value.is_empty())
}

#[cfg(test)]
#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
mod tests {
    pub(crate) fn run_env_test<T, F>(env_vars: T, f: F)
    where
        F: FnOnce(),
        T: Into<Vec<(&'static str, &'static str)>>,
    {
        temp_env::with_vars(
            env_vars
                .into()
                .iter()
                .map(|&(k, v)| (k, Some(v)))
                .collect::<Vec<(&'static str, Option<&'static str>)>>(),
            f,
        )
    }

    #[cfg(any(feature = "http-proto", feature = "http-json"))]
    #[test]
    fn test_default_http_endpoint() {
        let exporter_builder = crate::HttpExporterBuilder::default();

        assert_eq!(exporter_builder.exporter_config.endpoint, None);
    }

    #[cfg(feature = "logs")]
    #[cfg(any(feature = "http-proto", feature = "http-json"))]
    #[test]
    fn export_builder_error_invalid_http_endpoint() {
        use crate::{LogExporter, WithExportConfig};

        let exporter_result = LogExporter::builder()
            .with_http()
            .with_endpoint("invalid_uri/something")
            .with_timeout(std::time::Duration::from_secs(10))
            .build();

        assert!(
            matches!(
                exporter_result,
                Err(crate::exporter::ExporterBuildError::InvalidUri(_, _))
            ),
            "Expected InvalidUri error, but got {exporter_result:?}"
        );
    }

    #[cfg(feature = "grpc-tonic")]
    #[tokio::test]
    async fn export_builder_error_invalid_grpc_endpoint() {
        use crate::{LogExporter, WithExportConfig};

        let exporter_result = LogExporter::builder()
            .with_tonic()
            .with_endpoint("invalid_uri/something")
            .with_timeout(std::time::Duration::from_secs(10))
            .build();

        assert!(matches!(
            exporter_result,
            Err(crate::exporter::ExporterBuildError::InvalidUri(_, _))
        ));
    }

    #[cfg(feature = "grpc-tonic")]
    #[test]
    fn test_default_tonic_endpoint() {
        let exporter_builder = crate::TonicExporterBuilder::default();

        assert_eq!(exporter_builder.exporter_config.endpoint, None);
    }

    #[test]
    fn test_default_protocol() {
        #[cfg(all(
            feature = "http-json",
            not(any(feature = "grpc-tonic", feature = "http-proto"))
        ))]
        {
            assert_eq!(crate::Protocol::default(), crate::Protocol::HttpJson);
        }

        #[cfg(all(
            feature = "http-proto",
            not(any(feature = "grpc-tonic", feature = "http-json"))
        ))]
        {
            assert_eq!(crate::Protocol::default(), crate::Protocol::HttpBinary);
        }

        #[cfg(all(
            feature = "grpc-tonic",
            not(any(feature = "http-proto", feature = "http-json"))
        ))]
        {
            assert_eq!(crate::exporter::default_protocol(), crate::Protocol::Grpc);
        }
    }

    #[test]
    fn test_protocol_from_env() {
        use crate::{Protocol, OTEL_EXPORTER_OTLP_PROTOCOL};

        // Test with no env var set - should return None
        temp_env::with_var_unset(OTEL_EXPORTER_OTLP_PROTOCOL, || {
            assert_eq!(Protocol::from_env(), None);
        });

        // Test with grpc protocol
        #[cfg(feature = "grpc-tonic")]
        run_env_test(vec![(OTEL_EXPORTER_OTLP_PROTOCOL, "grpc")], || {
            assert_eq!(Protocol::from_env(), Some(Protocol::Grpc));
        });

        // Test with http/protobuf protocol
        #[cfg(feature = "http-proto")]
        run_env_test(vec![(OTEL_EXPORTER_OTLP_PROTOCOL, "http/protobuf")], || {
            assert_eq!(Protocol::from_env(), Some(Protocol::HttpBinary));
        });

        // Test with http/json protocol
        #[cfg(feature = "http-json")]
        run_env_test(vec![(OTEL_EXPORTER_OTLP_PROTOCOL, "http/json")], || {
            assert_eq!(Protocol::from_env(), Some(Protocol::HttpJson));
        });

        // Test with invalid protocol - should return None
        run_env_test(vec![(OTEL_EXPORTER_OTLP_PROTOCOL, "invalid")], || {
            assert_eq!(Protocol::from_env(), None);
        });
    }

    #[test]
    fn test_default_protocol_ignores_env() {
        // Protocol::default() should always return the feature-based default,
        // NOT consult environment variables. Env var resolution is handled
        // by resolve_protocol().

        // Without any env vars, default() equals feature_default()
        run_env_test(vec![], || {
            assert_eq!(
                crate::Protocol::default(),
                crate::Protocol::feature_default()
            );
        });

        // Even with a valid env var set, default() still equals feature_default()
        #[cfg(all(feature = "grpc-tonic", feature = "http-json"))]
        run_env_test(vec![(crate::OTEL_EXPORTER_OTLP_PROTOCOL, "grpc")], || {
            assert_eq!(
                crate::Protocol::default(),
                crate::Protocol::feature_default()
            );
        });
    }

    #[test]
    fn test_url_decode() {
        let test_cases = vec![
            // Format: (encoded, expected_decoded)
            ("v%201", Some("v 1")),
            ("v 1", Some("v 1")),
            ("%C3%B6%C3%A0%C2%A7%C3%96abcd%C3%84", Some("öà§ÖabcdÄ")),
            ("v%XX1", None),
        ];

        for (encoded, expected_decoded) in test_cases {
            assert_eq!(
                super::url_decode(encoded),
                expected_decoded.map(|v| v.to_string()),
            )
        }
    }

    #[test]
    fn test_parse_header_string() {
        let test_cases = vec![
            // Format: (input_str, expected_headers)
            ("k1=v1", vec![("k1", "v1")]),
            ("k1=v1,k2=v2", vec![("k1", "v1"), ("k2", "v2")]),
            ("k1=v1=10,k2,k3", vec![("k1", "v1=10")]),
            ("k1=v1,,,k2,k3=10", vec![("k1", "v1"), ("k3", "10")]),
        ];

        for (input_str, expected_headers) in test_cases {
            assert_eq!(
                super::parse_header_string(input_str).collect::<Vec<_>>(),
                expected_headers
                    .into_iter()
                    .map(|(k, v)| (k, v.to_string()))
                    .collect::<Vec<_>>(),
            )
        }
    }

    #[test]
    fn test_parse_header_key_value_string() {
        let test_cases = vec![
            // Format: (input_str, expected_header)
            ("k1=v1", Some(("k1", "v1"))),
            (
                "Authentication=Basic AAA",
                Some(("Authentication", "Basic AAA")),
            ),
            (
                "Authentication=Basic%20AAA",
                Some(("Authentication", "Basic AAA")),
            ),
            ("k1=%XX", Some(("k1", "%XX"))),
            ("", None),
            ("=v1", None),
            ("k1=", None),
        ];

        for (input_str, expected_headers) in test_cases {
            assert_eq!(
                super::parse_header_key_value_string(input_str),
                expected_headers.map(|(k, v)| (k, v.to_string())),
            )
        }
    }

    #[test]
    fn test_priority_of_signal_env_over_generic_env_for_timeout() {
        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT, "3000"),
                (super::OTEL_EXPORTER_OTLP_TIMEOUT, "2000"),
            ],
            || {
                let timeout =
                    super::resolve_timeout(crate::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT, None);
                assert_eq!(timeout.as_millis(), 3000);
            },
        );
    }

    #[test]
    fn test_priority_of_code_based_config_over_envs_for_timeout() {
        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT, "3000"),
                (super::OTEL_EXPORTER_OTLP_TIMEOUT, "2000"),
            ],
            || {
                let timeout = super::resolve_timeout(
                    crate::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT,
                    Some(&std::time::Duration::from_millis(1000)),
                );
                assert_eq!(timeout.as_millis(), 1000);
            },
        );
    }

    #[test]
    fn test_use_default_when_others_missing_for_timeout() {
        run_env_test(vec![], || {
            let timeout = super::resolve_timeout(crate::OTEL_EXPORTER_OTLP_TRACES_TIMEOUT, None);
            assert_eq!(timeout.as_millis(), 10_000);
        });
    }

    #[test]
    fn test_protocol_parse_from_env_var() {
        use crate::Protocol;

        // Test with custom env var name
        temp_env::with_var_unset("MY_CUSTOM_PROTOCOL_VAR", || {
            assert_eq!(Protocol::parse_from_env_var("MY_CUSTOM_PROTOCOL_VAR"), None);
        });

        #[cfg(feature = "http-proto")]
        run_env_test(vec![("MY_CUSTOM_PROTOCOL_VAR", "http/protobuf")], || {
            assert_eq!(
                Protocol::parse_from_env_var("MY_CUSTOM_PROTOCOL_VAR"),
                Some(Protocol::HttpBinary)
            );
        });

        #[cfg(feature = "grpc-tonic")]
        run_env_test(vec![("MY_CUSTOM_PROTOCOL_VAR", "grpc")], || {
            assert_eq!(
                Protocol::parse_from_env_var("MY_CUSTOM_PROTOCOL_VAR"),
                Some(Protocol::Grpc)
            );
        });

        // Invalid value returns None
        run_env_test(vec![("MY_CUSTOM_PROTOCOL_VAR", "invalid")], || {
            assert_eq!(Protocol::parse_from_env_var("MY_CUSTOM_PROTOCOL_VAR"), None);
        });
    }

    #[cfg(feature = "http-proto")]
    #[test]
    fn test_resolve_protocol_signal_env_overrides_generic() {
        use crate::Protocol;

        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, "http/protobuf"),
                (crate::OTEL_EXPORTER_OTLP_PROTOCOL, "grpc"),
            ],
            || {
                let protocol =
                    super::resolve_protocol(crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, None);
                assert_eq!(protocol, Protocol::HttpBinary);
            },
        );
    }

    #[cfg(feature = "http-proto")]
    #[test]
    fn test_resolve_protocol_code_overrides_all_envs() {
        use crate::Protocol;

        run_env_test(
            vec![
                (crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, "grpc"),
                (crate::OTEL_EXPORTER_OTLP_PROTOCOL, "grpc"),
            ],
            || {
                let protocol = super::resolve_protocol(
                    crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL,
                    Some(Protocol::HttpBinary),
                );
                assert_eq!(protocol, Protocol::HttpBinary);
            },
        );
    }

    #[cfg(all(feature = "grpc-tonic", feature = "http-proto"))]
    #[test]
    fn test_resolve_protocol_falls_back_to_generic_env() {
        use crate::Protocol;

        run_env_test(vec![(crate::OTEL_EXPORTER_OTLP_PROTOCOL, "grpc")], || {
            let protocol = super::resolve_protocol(crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, None);
            assert_eq!(protocol, Protocol::Grpc);
        });
    }

    #[test]
    fn test_resolve_protocol_falls_back_to_feature_default() {
        use crate::Protocol;

        run_env_test(vec![], || {
            let protocol = super::resolve_protocol(crate::OTEL_EXPORTER_OTLP_TRACES_PROTOCOL, None);
            assert_eq!(protocol, Protocol::feature_default());
        });
    }
}
