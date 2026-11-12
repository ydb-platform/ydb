//! Error classification for OTLP exporters with protocol-specific throttling support.
//!
//! This module provides error classification functions for HTTP and gRPC protocols,
//! supporting server-provided throttling hints like HTTP Retry-After headers and
//! gRPC RetryInfo metadata.

use crate::retry::RetryErrorType;

#[cfg(feature = "grpc-tonic")]
use tonic;

#[cfg(feature = "grpc-tonic")]
use tonic_types::StatusExt;

/// HTTP-specific error classification with Retry-After header support.
#[cfg(feature = "experimental-http-retry")]
pub mod http {
    use super::*;
    use std::time::Duration;

    /// Classifies HTTP errors based on status code and headers.
    ///
    /// # Arguments
    /// * `status_code` - HTTP status code
    /// * `retry_after_header` - Value of the Retry-After header, if present
    ///
    /// # Retry-After Header Formats
    /// * Seconds: "120"
    /// * HTTP Date: "Fri, 31 Dec 1999 23:59:59 GMT"
    pub fn classify_http_error(
        status_code: u16,
        retry_after_header: Option<&str>,
    ) -> RetryErrorType {
        match status_code {
            // 429 Too Many Requests - check for Retry-After
            429 => {
                if let Some(retry_after) = retry_after_header {
                    if let Some(duration) = parse_retry_after(retry_after) {
                        return RetryErrorType::Throttled(duration);
                    }
                }
                // Fallback to retryable if no valid Retry-After
                RetryErrorType::Retryable
            }
            // 5xx Server errors - retryable
            500..=599 => RetryErrorType::Retryable,
            // 4xx Client errors (except 429) - not retryable
            400..=499 => RetryErrorType::NonRetryable,
            // Other codes - retryable (network issues, etc.)
            _ => RetryErrorType::Retryable,
        }
    }

    /// Parses the Retry-After header value.
    ///
    /// Supports both formats:
    /// - Delay seconds: "120"
    /// - HTTP date: "Fri, 31 Dec 1999 23:59:59 GMT"
    ///
    /// Returns None if parsing fails or delay is unreasonable.
    fn parse_retry_after(retry_after: &str) -> Option<Duration> {
        // Try parsing as seconds first
        if let Ok(seconds) = retry_after.trim().parse::<u64>() {
            // Cap at 10 minutes. TODO - what's sensible here?
            let capped_seconds = seconds.min(600);
            return Some(Duration::from_secs(capped_seconds));
        }

        // Try parsing as HTTP date
        if let Ok(delay_seconds) = parse_http_date_to_delay(retry_after) {
            // Cap at 10 minutes. TODO - what's sensible here?
            let capped_seconds = delay_seconds.min(600);
            return Some(Duration::from_secs(capped_seconds));
        }

        None
    }

    /// Parses HTTP date format and returns delay in seconds from now.
    fn parse_http_date_to_delay(date_str: &str) -> Result<u64, ()> {
        use std::time::SystemTime;

        // Try parse the date; if we fail, propagate an () error up to the caller.
        let target_time = httpdate::parse_http_date(date_str).map_err(|_| ())?;

        let now = SystemTime::now();
        let delay = target_time
            .duration_since(now)
            .unwrap_or(std::time::Duration::ZERO);
        Ok(delay.as_secs())
    }
}

/// gRPC-specific error classification with RetryInfo support.
#[cfg(feature = "grpc-tonic")]
pub mod grpc {
    use super::*;

    /// Classifies a tonic::Status error
    #[cfg(feature = "grpc-tonic")]
    pub fn classify_tonic_status(status: &tonic::Status) -> RetryErrorType {
        // Use tonic-types to extract RetryInfo - this is the proper way!
        let retry_info_seconds = status
            .get_details_retry_info()
            .and_then(|retry_info| retry_info.retry_delay)
            .map(|duration| duration.as_secs());

        classify_grpc_error(status.code(), retry_info_seconds)
    }

    /// Classifies gRPC errors based on status code and metadata.
    ///
    /// Implements the OpenTelemetry OTLP specification for error handling:
    /// https://opentelemetry.io/docs/specs/otlp/
    /// https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#failures
    ///
    /// # Arguments
    /// * `grpc_code` - gRPC status code as tonic::Code enum
    /// * `retry_info_seconds` - Parsed retry delay from RetryInfo metadata, if present
    fn classify_grpc_error(
        grpc_code: tonic::Code,
        retry_info_seconds: Option<u64>,
    ) -> RetryErrorType {
        match grpc_code {
            // RESOURCE_EXHAUSTED: Special case per OTLP spec
            // Retryable only if server provides RetryInfo indicating recovery is possible
            tonic::Code::ResourceExhausted => {
                if let Some(seconds) = retry_info_seconds {
                    // Server signals recovery is possible - use throttled retry
                    let capped_seconds = seconds.min(600); // Cap at 10 minutes. TODO - what's sensible here?
                    return RetryErrorType::Throttled(std::time::Duration::from_secs(
                        capped_seconds,
                    ));
                }
                // No RetryInfo - treat as non-retryable per OTLP spec
                RetryErrorType::NonRetryable
            }

            // Retryable errors per OTLP specification
            tonic::Code::Cancelled
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Aborted
            | tonic::Code::OutOfRange
            | tonic::Code::Unavailable
            | tonic::Code::DataLoss => RetryErrorType::Retryable,

            // Non-retryable errors per OTLP specification
            tonic::Code::Unknown
            | tonic::Code::InvalidArgument
            | tonic::Code::NotFound
            | tonic::Code::AlreadyExists
            | tonic::Code::PermissionDenied
            | tonic::Code::FailedPrecondition
            | tonic::Code::Unimplemented
            | tonic::Code::Internal
            | tonic::Code::Unauthenticated => RetryErrorType::NonRetryable,

            // OK should never reach here in error scenarios, but handle gracefully
            tonic::Code::Ok => RetryErrorType::NonRetryable,
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests for HTTP error classification
    #[cfg(feature = "experimental-http-retry")]
    mod http_tests {
        use crate::retry::RetryErrorType;
        use crate::retry_classification::http::*;
        use std::time::Duration;

        #[test]
        fn test_http_429_with_retry_after_seconds() {
            let result = classify_http_error(429, Some("30"));
            assert_eq!(result, RetryErrorType::Throttled(Duration::from_secs(30)));
        }

        #[test]
        fn test_http_429_with_large_retry_after_capped() {
            let result = classify_http_error(429, Some("900")); // 15 minutes
            assert_eq!(
                result,
                RetryErrorType::Throttled(std::time::Duration::from_secs(600))
            ); // Capped at 10 minutes
        }

        #[test]
        fn test_http_429_with_invalid_retry_after() {
            let result = classify_http_error(429, Some("invalid"));
            assert_eq!(result, RetryErrorType::Retryable); // Fallback
        }

        #[test]
        fn test_http_429_without_retry_after() {
            let result = classify_http_error(429, None);
            assert_eq!(result, RetryErrorType::Retryable); // Fallback
        }

        #[test]
        fn test_http_5xx_errors() {
            assert_eq!(classify_http_error(500, None), RetryErrorType::Retryable);
            assert_eq!(classify_http_error(502, None), RetryErrorType::Retryable);
            assert_eq!(classify_http_error(503, None), RetryErrorType::Retryable);
            assert_eq!(classify_http_error(599, None), RetryErrorType::Retryable);
        }

        #[test]
        fn test_http_4xx_errors() {
            assert_eq!(classify_http_error(400, None), RetryErrorType::NonRetryable);
            assert_eq!(classify_http_error(401, None), RetryErrorType::NonRetryable);
            assert_eq!(classify_http_error(403, None), RetryErrorType::NonRetryable);
            assert_eq!(classify_http_error(404, None), RetryErrorType::NonRetryable);
            assert_eq!(classify_http_error(499, None), RetryErrorType::NonRetryable);
        }

        #[test]
        fn test_http_other_errors() {
            assert_eq!(classify_http_error(100, None), RetryErrorType::Retryable);
            assert_eq!(classify_http_error(200, None), RetryErrorType::Retryable);
            assert_eq!(classify_http_error(300, None), RetryErrorType::Retryable);
        }

        #[test]
        #[cfg(feature = "experimental-http-retry")]
        fn test_http_429_with_retry_after_valid_date() {
            use std::time::SystemTime;

            // Create a time 30 seconds in the future
            let future_time = SystemTime::now() + Duration::from_secs(30);
            let date_str = httpdate::fmt_http_date(future_time);
            let result = classify_http_error(429, Some(&date_str));
            match result {
                RetryErrorType::Throttled(duration) => {
                    let secs = duration.as_secs();
                    assert!(
                        (29..=30).contains(&secs),
                        "Expected ~30 seconds, got {}",
                        secs
                    );
                }
                _ => panic!("Expected Throttled, got {:?}", result),
            }
        }

        #[test]
        #[cfg(feature = "experimental-http-retry")]
        fn test_http_429_with_retry_after_invalid_date() {
            let result = classify_http_error(429, Some("Not a valid date"));
            assert_eq!(result, RetryErrorType::Retryable); // Falls back to retryable
        }

        #[test]
        #[cfg(feature = "experimental-http-retry")]
        fn test_http_429_with_retry_after_malformed_date() {
            let result = classify_http_error(429, Some("Sun, 99 Nov 9999 99:99:99 GMT"));
            assert_eq!(result, RetryErrorType::Retryable); // Falls back to retryable
        }
    }

    // Tests for gRPC error classification using public interface
    #[cfg(feature = "grpc-tonic")]
    mod grpc_tests {
        use crate::retry::RetryErrorType;
        use crate::retry_classification::grpc::classify_tonic_status;
        use tonic_types::{ErrorDetails, StatusExt};

        #[test]
        fn test_grpc_resource_exhausted_with_retry_info() {
            let error_details =
                ErrorDetails::with_retry_info(Some(std::time::Duration::from_secs(45)));
            let status = tonic::Status::with_error_details(
                tonic::Code::ResourceExhausted,
                "rate limited",
                error_details,
            );
            let result = classify_tonic_status(&status);
            assert_eq!(
                result,
                RetryErrorType::Throttled(std::time::Duration::from_secs(45))
            );
        }

        #[test]
        fn test_grpc_resource_exhausted_with_large_retry_info_capped() {
            let error_details =
                ErrorDetails::with_retry_info(Some(std::time::Duration::from_secs(900))); // 15 minutes
            let status = tonic::Status::with_error_details(
                tonic::Code::ResourceExhausted,
                "rate limited",
                error_details,
            );
            let result = classify_tonic_status(&status);
            assert_eq!(
                result,
                RetryErrorType::Throttled(std::time::Duration::from_secs(600))
            ); // Capped at 10 minutes
        }

        #[test]
        fn test_grpc_resource_exhausted_without_retry_info() {
            let status = tonic::Status::new(tonic::Code::ResourceExhausted, "rate limited");
            let result = classify_tonic_status(&status);
            // Per OTLP spec: RESOURCE_EXHAUSTED without RetryInfo is non-retryable
            assert_eq!(result, RetryErrorType::NonRetryable);
        }

        #[test]
        fn test_grpc_retryable_errors() {
            // Test all retryable errors per OTLP specification
            let cancelled = tonic::Status::new(tonic::Code::Cancelled, "cancelled");
            assert_eq!(classify_tonic_status(&cancelled), RetryErrorType::Retryable);

            let deadline_exceeded =
                tonic::Status::new(tonic::Code::DeadlineExceeded, "deadline exceeded");
            assert_eq!(
                classify_tonic_status(&deadline_exceeded),
                RetryErrorType::Retryable
            );

            let aborted = tonic::Status::new(tonic::Code::Aborted, "aborted");
            assert_eq!(classify_tonic_status(&aborted), RetryErrorType::Retryable);

            let out_of_range = tonic::Status::new(tonic::Code::OutOfRange, "out of range");
            assert_eq!(
                classify_tonic_status(&out_of_range),
                RetryErrorType::Retryable
            );

            let unavailable = tonic::Status::new(tonic::Code::Unavailable, "unavailable");
            assert_eq!(
                classify_tonic_status(&unavailable),
                RetryErrorType::Retryable
            );

            let data_loss = tonic::Status::new(tonic::Code::DataLoss, "data loss");
            assert_eq!(classify_tonic_status(&data_loss), RetryErrorType::Retryable);
        }

        #[test]
        fn test_grpc_non_retryable_errors() {
            // Test all non-retryable errors per OTLP specification
            let unknown = tonic::Status::new(tonic::Code::Unknown, "unknown");
            assert_eq!(
                classify_tonic_status(&unknown),
                RetryErrorType::NonRetryable
            );

            let invalid_argument =
                tonic::Status::new(tonic::Code::InvalidArgument, "invalid argument");
            assert_eq!(
                classify_tonic_status(&invalid_argument),
                RetryErrorType::NonRetryable
            );

            let not_found = tonic::Status::new(tonic::Code::NotFound, "not found");
            assert_eq!(
                classify_tonic_status(&not_found),
                RetryErrorType::NonRetryable
            );

            let already_exists = tonic::Status::new(tonic::Code::AlreadyExists, "already exists");
            assert_eq!(
                classify_tonic_status(&already_exists),
                RetryErrorType::NonRetryable
            );

            let permission_denied =
                tonic::Status::new(tonic::Code::PermissionDenied, "permission denied");
            assert_eq!(
                classify_tonic_status(&permission_denied),
                RetryErrorType::NonRetryable
            );

            let failed_precondition =
                tonic::Status::new(tonic::Code::FailedPrecondition, "failed precondition");
            assert_eq!(
                classify_tonic_status(&failed_precondition),
                RetryErrorType::NonRetryable
            );

            let unimplemented = tonic::Status::new(tonic::Code::Unimplemented, "unimplemented");
            assert_eq!(
                classify_tonic_status(&unimplemented),
                RetryErrorType::NonRetryable
            );

            let internal = tonic::Status::new(tonic::Code::Internal, "internal error");
            assert_eq!(
                classify_tonic_status(&internal),
                RetryErrorType::NonRetryable
            );

            let unauthenticated =
                tonic::Status::new(tonic::Code::Unauthenticated, "unauthenticated");
            assert_eq!(
                classify_tonic_status(&unauthenticated),
                RetryErrorType::NonRetryable
            );
        }

        #[test]
        fn test_grpc_ok_code_handled() {
            // OK status should be handled gracefully (though unlikely in error scenarios)
            let ok = tonic::Status::new(tonic::Code::Ok, "success");
            assert_eq!(classify_tonic_status(&ok), RetryErrorType::NonRetryable);
        }

        // Tests for tonic-types RetryInfo integration
        #[cfg(feature = "grpc-tonic")]
        mod retry_info_tests {
            use super::*;
            use crate::retry_classification::grpc::classify_tonic_status;
            use tonic_types::{ErrorDetails, StatusExt};

            #[test]
            fn test_classify_status_with_retry_info() {
                // Create a tonic::Status with RetryInfo using proper StatusExt API
                let error_details =
                    ErrorDetails::with_retry_info(Some(std::time::Duration::from_secs(30)));
                let status = tonic::Status::with_error_details(
                    tonic::Code::ResourceExhausted,
                    "rate limited",
                    error_details,
                );

                // Test classification
                let result = classify_tonic_status(&status);
                assert_eq!(
                    result,
                    RetryErrorType::Throttled(std::time::Duration::from_secs(30))
                );
            }

            #[test]
            fn test_classify_status_with_fractional_retry_info() {
                // Create a tonic::Status with fractional seconds RetryInfo
                let error_details =
                    ErrorDetails::with_retry_info(Some(std::time::Duration::from_millis(5500))); // 5.5 seconds
                let status = tonic::Status::with_error_details(
                    tonic::Code::ResourceExhausted,
                    "rate limited",
                    error_details,
                );

                // Should use exact duration (5.5s = 5s)
                let result = classify_tonic_status(&status);
                assert_eq!(
                    result,
                    RetryErrorType::Throttled(std::time::Duration::from_secs(5))
                );
            }

            #[test]
            fn test_classify_status_without_retry_info() {
                // Status with resource_exhausted but no RetryInfo
                let status = tonic::Status::new(tonic::Code::ResourceExhausted, "rate limited");

                // Per OTLP spec: should be non-retryable without RetryInfo
                let result = classify_tonic_status(&status);
                assert_eq!(result, RetryErrorType::NonRetryable);
            }

            #[test]
            fn test_classify_status_non_retryable_error() {
                // Status with non-retryable error code
                let status = tonic::Status::new(tonic::Code::InvalidArgument, "bad request");

                let result = classify_tonic_status(&status);
                assert_eq!(result, RetryErrorType::NonRetryable);
            }

            #[test]
            fn test_classify_status_retryable_error() {
                // Status with retryable error code
                let status = tonic::Status::new(tonic::Code::Unavailable, "service unavailable");

                let result = classify_tonic_status(&status);
                assert_eq!(result, RetryErrorType::Retryable);
            }

            #[test]
            fn test_classify_status_large_retry_delay() {
                // Test with large retry delay - should be capped at 10 minutes
                let error_details =
                    ErrorDetails::with_retry_info(Some(std::time::Duration::from_secs(3600))); // 1 hour
                let status = tonic::Status::with_error_details(
                    tonic::Code::ResourceExhausted,
                    "rate limited",
                    error_details,
                );

                let result = classify_tonic_status(&status);
                // Should be capped at 10 minutes (600 seconds)
                assert_eq!(
                    result,
                    RetryErrorType::Throttled(std::time::Duration::from_secs(600))
                );
            }

            #[test]
            fn test_status_ext_get_details() {
                // Test that StatusExt works correctly
                let error_details =
                    ErrorDetails::with_retry_info(Some(std::time::Duration::from_secs(45)));
                let status = tonic::Status::with_error_details(
                    tonic::Code::ResourceExhausted,
                    "rate limited",
                    error_details,
                );

                // Direct extraction should work
                let extracted = status.get_details_retry_info();
                assert!(extracted.is_some());

                let retry_delay = extracted.unwrap().retry_delay;
                assert_eq!(retry_delay, Some(std::time::Duration::from_secs(45)));
            }
        }
    }
}
