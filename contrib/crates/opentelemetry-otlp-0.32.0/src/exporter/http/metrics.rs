use crate::metric::MetricsClient;
use crate::Protocol;
use opentelemetry::{otel_debug, otel_warn};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
#[cfg(feature = "http-proto")]
use prost::Message;

use super::OtlpHttpClient;

impl MetricsClient for OtlpHttpClient {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let build_body_wrapper = |client: &OtlpHttpClient, metrics: &ResourceMetrics| {
            client
                .build_metrics_export_body(metrics)
                .ok_or_else(|| "Failed to serialize metrics".to_string())
        };

        let response_body = self
            .export_http_with_retry(metrics, build_body_wrapper, "HttpMetricsClient.Export")
            .await?;

        handle_partial_success(&response_body, self.protocol);
        Ok(())
    }

    fn shutdown(&self) -> OTelSdkResult {
        self.client
            .lock()
            .map_err(|e| OTelSdkError::InternalFailure(format!("Failed to acquire lock: {e}")))?
            .take();

        Ok(())
    }
}

/// Handles partial success returned by OTLP endpoints. We log the rejected data points,
/// as well as the error message returned.
fn handle_partial_success(response_body: &[u8], protocol: Protocol) {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;

    let response: ExportMetricsServiceResponse = match protocol {
        #[cfg(feature = "http-json")]
        Protocol::HttpJson => match serde_json::from_slice(response_body) {
            Ok(r) => r,
            Err(e) => {
                otel_debug!(name: "HttpMetricsClient.ResponseParseError", error = e.to_string());
                return;
            }
        },
        #[cfg(feature = "http-proto")]
        Protocol::HttpBinary => match Message::decode(response_body) {
            Ok(r) => r,
            Err(e) => {
                otel_debug!(name: "HttpMetricsClient.ResponseParseError", error = e.to_string());
                return;
            }
        },
        #[cfg(feature = "grpc-tonic")]
        Protocol::Grpc => {
            unreachable!("HTTP client should not receive Grpc protocol")
        }
    };

    if let Some(partial_success) = response.partial_success {
        if partial_success.rejected_data_points > 0 || !partial_success.error_message.is_empty() {
            otel_warn!(
                name: "HttpMetricsClient.PartialSuccess",
                rejected_data_points = partial_success.rejected_data_points,
                error_message = partial_success.error_message.as_str(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_invalid_protobuf() {
        // Corrupted/invalid protobuf data
        let invalid = vec![0xFF, 0xFF, 0xFF, 0xFF];

        // Should not panic - logs debug and returns early
        handle_partial_success(&invalid, Protocol::HttpBinary);
    }

    #[test]
    fn test_handle_empty_response() {
        let empty = vec![];

        // Should not panic
        handle_partial_success(&empty, Protocol::HttpBinary);
    }

    #[cfg(feature = "http-json")]
    #[test]
    fn test_handle_invalid_json() {
        let invalid_json = b"{not valid json}";

        // Should not panic - logs debug and returns
        handle_partial_success(invalid_json, Protocol::HttpJson);
    }
}
