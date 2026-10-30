//! OTLP - Log Exporter
//!
//! Defines a [LogExporter] to send logs via the OpenTelemetry Protocol (OTLP)

#[cfg(feature = "grpc-tonic")]
use opentelemetry::otel_debug;
use opentelemetry_sdk::{error::OTelSdkResult, logs::LogBatch};
use std::fmt::Debug;
use std::time;

use crate::{exporter::HasExportConfig, ExporterBuildError, NoExporterBuilderSet};

#[cfg(feature = "grpc-tonic")]
use crate::{exporter::tonic::HasTonicConfig, TonicExporterBuilder, TonicExporterBuilderSet};

#[cfg(any(feature = "http-proto", feature = "http-json"))]
use crate::{exporter::http::HasHttpConfig, HttpExporterBuilder, HttpExporterBuilderSet};

/// Compression algorithm to use, defaults to none.
pub const OTEL_EXPORTER_OTLP_LOGS_COMPRESSION: &str = "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION";

/// Target to which the exporter is going to send logs
pub const OTEL_EXPORTER_OTLP_LOGS_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

/// Maximum time the OTLP exporter will wait for each batch logs export.
pub const OTEL_EXPORTER_OTLP_LOGS_TIMEOUT: &str = "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT";

/// Key-value pairs to be used as headers associated with gRPC or HTTP requests
/// for sending logs.
/// Example: `k1=v1,k2=v2`
/// Note: this is only supported for HTTP.
pub const OTEL_EXPORTER_OTLP_LOGS_HEADERS: &str = "OTEL_EXPORTER_OTLP_LOGS_HEADERS";
/// Protocol to use for log exports. Valid values: `grpc`, `http/protobuf`, `http/json`.
pub const OTEL_EXPORTER_OTLP_LOGS_PROTOCOL: &str = "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL";

/// Builder for creating a new [LogExporter].
#[derive(Debug, Default, Clone)]
pub struct LogExporterBuilder<C> {
    client: C,
    endpoint: Option<String>,
}

impl LogExporterBuilder<NoExporterBuilderSet> {
    /// Create a new [LogExporterBuilder] with default settings.
    pub fn new() -> Self {
        LogExporterBuilder::default()
    }

    /// With the gRPC Tonic transport.
    #[cfg(feature = "grpc-tonic")]
    pub fn with_tonic(self) -> LogExporterBuilder<TonicExporterBuilderSet> {
        LogExporterBuilder {
            client: TonicExporterBuilderSet(TonicExporterBuilder::default()),
            endpoint: self.endpoint,
        }
    }

    /// With the HTTP transport.
    #[cfg(any(feature = "http-proto", feature = "http-json"))]
    pub fn with_http(self) -> LogExporterBuilder<HttpExporterBuilderSet> {
        LogExporterBuilder {
            client: HttpExporterBuilderSet(HttpExporterBuilder::default()),
            endpoint: self.endpoint,
        }
    }

    /// Build the [LogExporter] with the default transport selected by environment
    /// variable or feature flags.
    ///
    /// The transport is chosen based on:
    /// 1. `OTEL_EXPORTER_OTLP_LOGS_PROTOCOL` environment variable
    /// 2. `OTEL_EXPORTER_OTLP_PROTOCOL` environment variable
    /// 3. Enabled features, with priority: `http-json` > `http-proto` > `grpc-tonic`
    ///
    /// Use [`with_tonic`](Self::with_tonic) or [`with_http`](Self::with_http) to
    /// explicitly select a transport and access transport-specific configuration.
    #[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
    pub fn build(self) -> Result<LogExporter, ExporterBuildError> {
        // NOTE: The transport-specific builder will call resolve_protocol again
        // internally (for HTTP sub-protocol selection or tonic validation), but
        // that's harmless — the result is the same.
        let protocol = crate::exporter::resolve_protocol(OTEL_EXPORTER_OTLP_LOGS_PROTOCOL, None);
        match protocol {
            #[cfg(feature = "grpc-tonic")]
            crate::Protocol::Grpc => self.with_tonic().build(),
            #[cfg(feature = "http-proto")]
            crate::Protocol::HttpBinary => self.with_http().build(),
            #[cfg(feature = "http-json")]
            crate::Protocol::HttpJson => self.with_http().build(),
        }
    }
}

#[cfg(feature = "grpc-tonic")]
impl LogExporterBuilder<TonicExporterBuilderSet> {
    /// Build the [LogExporter] with the gRPC Tonic transport.
    pub fn build(self) -> Result<LogExporter, ExporterBuildError> {
        let result = self.client.0.build_log_exporter();
        otel_debug!(name: "LogExporterBuilt", result = format!("{:?}", &result));
        result
    }
}

#[cfg(any(feature = "http-proto", feature = "http-json"))]
impl LogExporterBuilder<HttpExporterBuilderSet> {
    /// Build the [LogExporter] with the HTTP transport.
    pub fn build(self) -> Result<LogExporter, ExporterBuildError> {
        self.client.0.build_log_exporter()
    }
}

#[cfg(feature = "grpc-tonic")]
impl HasExportConfig for LogExporterBuilder<TonicExporterBuilderSet> {
    fn export_config(&mut self) -> &mut crate::exporter::ExportConfig {
        &mut self.client.0.exporter_config
    }
}

#[cfg(any(feature = "http-proto", feature = "http-json"))]
impl HasExportConfig for LogExporterBuilder<HttpExporterBuilderSet> {
    fn export_config(&mut self) -> &mut crate::exporter::ExportConfig {
        &mut self.client.0.exporter_config
    }
}

#[cfg(feature = "grpc-tonic")]
impl HasTonicConfig for LogExporterBuilder<TonicExporterBuilderSet> {
    fn tonic_config(&mut self) -> &mut crate::exporter::tonic::TonicConfig {
        &mut self.client.0.tonic_config
    }
}

#[cfg(any(feature = "http-proto", feature = "http-json"))]
impl HasHttpConfig for LogExporterBuilder<HttpExporterBuilderSet> {
    fn http_client_config(&mut self) -> &mut crate::exporter::http::HttpConfig {
        &mut self.client.0.http_config
    }
}

/// OTLP exporter that sends log data
#[derive(Debug)]
pub struct LogExporter {
    client: SupportedTransportClient,
}

#[derive(Debug)]
enum SupportedTransportClient {
    #[cfg(feature = "grpc-tonic")]
    Tonic(crate::exporter::tonic::logs::TonicLogsClient),
    #[cfg(any(feature = "http-proto", feature = "http-json"))]
    Http(crate::exporter::http::OtlpHttpClient),
}

impl LogExporter {
    /// Obtain a builder to configure a [LogExporter].
    pub fn builder() -> LogExporterBuilder<NoExporterBuilderSet> {
        LogExporterBuilder::default()
    }

    #[cfg(any(feature = "http-proto", feature = "http-json"))]
    pub(crate) fn from_http(client: crate::exporter::http::OtlpHttpClient) -> Self {
        LogExporter {
            client: SupportedTransportClient::Http(client),
        }
    }

    #[cfg(feature = "grpc-tonic")]
    pub(crate) fn from_tonic(client: crate::exporter::tonic::logs::TonicLogsClient) -> Self {
        LogExporter {
            client: SupportedTransportClient::Tonic(client),
        }
    }
}

impl opentelemetry_sdk::logs::LogExporter for LogExporter {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        match &self.client {
            #[cfg(feature = "grpc-tonic")]
            SupportedTransportClient::Tonic(client) => client.export(batch).await,
            #[cfg(any(feature = "http-proto", feature = "http-json"))]
            SupportedTransportClient::Http(client) => client.export(batch).await,
        }
    }

    fn set_resource(&mut self, resource: &opentelemetry_sdk::Resource) {
        match &mut self.client {
            #[cfg(feature = "grpc-tonic")]
            SupportedTransportClient::Tonic(client) => client.set_resource(resource),
            #[cfg(any(feature = "http-proto", feature = "http-json"))]
            SupportedTransportClient::Http(client) => client.set_resource(resource),
        }
    }

    fn shutdown_with_timeout(&self, _timeout: time::Duration) -> OTelSdkResult {
        match &self.client {
            #[cfg(feature = "grpc-tonic")]
            SupportedTransportClient::Tonic(client) => client.shutdown(),
            #[cfg(any(feature = "http-proto", feature = "http-json"))]
            SupportedTransportClient::Http(client) => client.shutdown(),
        }
    }
}

#[cfg(test)]
#[cfg(any(feature = "grpc-tonic", feature = "http-proto", feature = "http-json"))]
mod tests {
    use crate::LogExporter;

    #[test]
    fn build_with_default_transport() {
        let result = LogExporter::builder().build();
        assert!(result.is_ok(), "build() should succeed: {:?}", result.err());
    }
}
