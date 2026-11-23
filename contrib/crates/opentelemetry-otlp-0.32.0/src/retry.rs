//! This module provides functionality for retrying operations with exponential backoff and jitter.
//!
//! The `RetryPolicy` struct defines the configuration for the retry behavior, including the maximum
//! number of retries, initial delay, maximum delay, and jitter.
//!
//! The `retry_with_backoff` function retries the given operation according to the
//! specified retry policy, using exponential backoff and jitter to determine the delay between
//! retries. The function uses error classification to determine retry behavior and can honor
//! server-provided throttling hints.
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use opentelemetry::{otel_debug, otel_info};

#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use opentelemetry::otel_warn;
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use opentelemetry_sdk::runtime::Runtime;
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use std::future::Future;
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use std::hash::{DefaultHasher, Hasher};
use std::time::Duration;
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
use std::time::SystemTime;

/// Classification of errors for retry purposes.
#[derive(Debug, Clone, PartialEq)]
pub enum RetryErrorType {
    /// Error is not retryable (e.g., authentication failure, bad request).
    NonRetryable,
    /// Error is retryable with exponential backoff (e.g., server error, network timeout).
    Retryable,
    /// Error indicates throttling - wait for the specified duration before retrying.
    /// This overrides exponential backoff timing.
    Throttled(Duration),
}

/// Configuration for retry policy.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_retries: usize,
    /// Initial delay in milliseconds before the first retry.
    pub initial_delay_ms: u64,
    /// Maximum delay in milliseconds between retries.
    pub max_delay_ms: u64,
    /// Maximum jitter in milliseconds to add to the delay.
    pub jitter_ms: u64,
}

// Generates a random jitter value up to max_jitter
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
fn generate_jitter(max_jitter: u64) -> u64 {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();

    let mut hasher = DefaultHasher::default();
    hasher.write_u32(nanos);
    hasher.finish() % (max_jitter + 1)
}

/// Retries the given operation with exponential backoff, jitter, and error classification.
///
/// This function provides sophisticated retry behavior by classifying errors
/// and honoring server-provided throttling hints (e.g., HTTP Retry-After, gRPC RetryInfo).
///
/// # Arguments
///
/// * `runtime` - The async runtime to use for delays.
/// * `policy` - The retry policy configuration.
/// * `error_classifier` - Function to classify errors for retry decisions.
/// * `operation_name` - The name of the operation being retried.
/// * `operation` - The operation to be retried.
///
/// # Returns
///
/// A `Result` containing the operation's result or an error if max retries are reached
/// or a non-retryable error occurs.
#[cfg(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
))]
pub async fn retry_with_backoff<R, F, Fut, T, E, C>(
    runtime: R,
    policy: RetryPolicy,
    error_classifier: C,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    R: Runtime,
    F: FnMut() -> Fut,
    E: std::fmt::Debug,
    Fut: Future<Output = Result<T, E>>,
    C: Fn(&E) -> RetryErrorType,
{
    let mut attempt = 0;
    let mut delay = policy.initial_delay_ms;

    loop {
        match operation().await {
            Ok(result) => return Ok(result), // Return the result if the operation succeeds
            Err(err) => {
                // Classify the error
                let error_type = error_classifier(&err);

                match error_type {
                    RetryErrorType::NonRetryable => {
                        otel_warn!(name: "Export.Failed.NonRetryable",
                            operation = operation_name,
                            message = "OTLP export failed with non-retryable error - telemetry data will be lost");
                        return Err(err);
                    }
                    RetryErrorType::Retryable if attempt < policy.max_retries => {
                        attempt += 1;
                        // Use exponential backoff with jitter
                        let jitter = generate_jitter(policy.jitter_ms);
                        let delay_with_jitter = std::cmp::min(delay + jitter, policy.max_delay_ms);
                        otel_debug!(name: "Export.InProgress.Retrying",
                            operation = operation_name,
                            attempt = attempt,
                            delay_ms = delay_with_jitter,
                            jitter_ms = jitter,
                            message = "OTLP export failed with retryable error - retrying"
                        );
                        runtime
                            .delay(Duration::from_millis(delay_with_jitter))
                            .await;
                        delay = std::cmp::min(delay * 2, policy.max_delay_ms); // Exponential backoff
                    }
                    RetryErrorType::Throttled(server_delay) if attempt < policy.max_retries => {
                        attempt += 1;
                        // Use server-specified delay (overrides exponential backoff)
                        otel_info!(name: "Export.InProgress.Throttled",
                            operation = operation_name,
                            attempt = attempt,
                            delay_ms = server_delay.as_millis(),
                            message = "OTLP export throttled by OTLP endpoint - delaying and retrying"
                        );
                        runtime.delay(server_delay).await;
                        // Don't update exponential backoff delay for next attempt since server provided specific timing
                    }
                    _ => {
                        // Max retries reached
                        otel_warn!(name: "Export.Failed.Exhausted",
                            operation = operation_name,
                            retries = attempt,
                            message = "OTLP export exhausted retries - telemetry data will be lost"
                        );
                        return Err(err);
                    }
                }
            }
        }
    }
}

/// No-op retry function for when experimental_async_runtime is not enabled.
/// This function will execute the operation exactly once without any retries.
#[cfg(not(any(
    feature = "experimental-grpc-retry",
    feature = "experimental-http-retry"
)))]
pub async fn retry_with_backoff<R, F, Fut, T, E, C>(
    _runtime: R,
    _policy: RetryPolicy,
    _error_classifier: C,
    _operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Without experimental_async_runtime, we just execute once without retries
    operation().await
}

#[cfg(all(test, feature = "experimental-grpc-retry"))]
mod tests {
    use super::*;
    use opentelemetry_sdk::runtime::Tokio;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::time::timeout;

    // Test to ensure generate_jitter returns a value within the expected range
    #[tokio::test]
    async fn test_generate_jitter() {
        let max_jitter = 100;
        let jitter = generate_jitter(max_jitter);
        assert!(jitter <= max_jitter);
    }

    // Test to ensure retry_with_exponential_backoff succeeds on the first attempt
    #[tokio::test]
    async fn test_retry_with_exponential_backoff_success() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let result = retry_with_backoff(
            runtime,
            policy,
            |_: &()| RetryErrorType::Retryable,
            "test_operation",
            || Box::pin(async { Ok::<_, ()>("success") }),
        )
        .await;

        assert_eq!(result, Ok("success"));
    }

    // Test to ensure retry_with_exponential_backoff retries the operation and eventually succeeds
    #[tokio::test]
    async fn test_retry_with_exponential_backoff_retries() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let attempts = AtomicUsize::new(0);

        let result = retry_with_backoff(
            runtime,
            policy,
            |_: &&str| RetryErrorType::Retryable,
            "test_operation",
            || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    if attempt < 2 {
                        Err::<&str, &str>("error") // Fail the first two attempts
                    } else {
                        Ok::<&str, &str>("success") // Succeed on the third attempt
                    }
                })
            },
        )
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Ensure there were 3 attempts
    }

    // Test to ensure retry_with_exponential_backoff fails after max retries
    #[tokio::test]
    async fn test_retry_with_exponential_backoff_failure() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let attempts = AtomicUsize::new(0);

        let result = retry_with_backoff(
            runtime,
            policy,
            |_: &&str| RetryErrorType::Retryable,
            "test_operation",
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err::<(), _>("error") }) // Always fail
            },
        )
        .await;

        assert_eq!(result, Err("error"));
        assert_eq!(attempts.load(Ordering::SeqCst), 4); // Ensure there were 4 attempts (initial + 3 retries)
    }

    // Test to ensure retry_with_exponential_backoff respects the timeout
    #[tokio::test]
    async fn test_retry_with_exponential_backoff_timeout() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 12, // Increase the number of retries
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let result = timeout(
            Duration::from_secs(1),
            retry_with_backoff(
                runtime,
                policy,
                |_: &&str| RetryErrorType::Retryable,
                "test_operation",
                || {
                    Box::pin(async { Err::<(), _>("error") }) // Always fail
                },
            ),
        )
        .await;

        assert!(result.is_err()); // Ensure the operation times out
    }

    // Tests for error classification (Phase 1)
    #[test]
    fn test_retry_error_type_equality() {
        assert_eq!(RetryErrorType::NonRetryable, RetryErrorType::NonRetryable);
        assert_eq!(RetryErrorType::Retryable, RetryErrorType::Retryable);
        assert_eq!(
            RetryErrorType::Throttled(Duration::from_secs(30)),
            RetryErrorType::Throttled(Duration::from_secs(30))
        );
        assert_ne!(RetryErrorType::Retryable, RetryErrorType::NonRetryable);
    }

    // Tests for enhanced retry function (Phase 3)
    #[tokio::test]
    async fn test_retry_with_throttling_non_retryable_error() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let attempts = AtomicUsize::new(0);

        // Classifier that returns non-retryable
        let classifier = |_: &()| RetryErrorType::NonRetryable;

        let result = retry_with_backoff(runtime, policy, classifier, "test_operation", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err::<(), _>(()) }) // Always fail
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // Should only try once
    }

    #[tokio::test]
    async fn test_retry_with_throttling_retryable_error() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 2,
            initial_delay_ms: 10, // Short delay for test
            max_delay_ms: 100,
            jitter_ms: 5,
        };

        let attempts = AtomicUsize::new(0);

        // Classifier that returns retryable
        let classifier = |_: &()| RetryErrorType::Retryable;

        let result = retry_with_backoff(runtime, policy, classifier, "test_operation", || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt < 1 {
                    Err::<&str, ()>(()) // Fail first attempt
                } else {
                    Ok("success") // Succeed on retry
                }
            })
        })
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // Should try twice
    }

    #[tokio::test]
    async fn test_retry_with_throttling_throttled_error() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 2,
            initial_delay_ms: 100,
            max_delay_ms: 1600,
            jitter_ms: 100,
        };

        let attempts = AtomicUsize::new(0);

        // Classifier that returns throttled with short delay
        let classifier = |_: &()| RetryErrorType::Throttled(Duration::from_millis(10));

        let start_time = std::time::Instant::now();

        let result = retry_with_backoff(runtime, policy, classifier, "test_operation", || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt < 1 {
                    Err::<&str, ()>(()) // Fail first attempt (will be throttled)
                } else {
                    Ok("success") // Succeed on retry
                }
            })
        })
        .await;

        let elapsed = start_time.elapsed();

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // Should try twice
        assert!(elapsed >= Duration::from_millis(10)); // Should have waited for throttle delay
    }

    #[tokio::test]
    async fn test_retry_with_throttling_max_attempts_exceeded() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 1, // Only 1 retry
            initial_delay_ms: 10,
            max_delay_ms: 100,
            jitter_ms: 5,
        };

        let attempts = AtomicUsize::new(0);

        // Classifier that returns retryable
        let classifier = |_: &()| RetryErrorType::Retryable;

        let result = retry_with_backoff(runtime, policy, classifier, "test_operation", || {
            attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err::<(), _>(()) }) // Always fail
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // Initial attempt + 1 retry
    }

    #[tokio::test]
    async fn test_retry_with_throttling_mixed_error_types() {
        let runtime = Tokio;
        let policy = RetryPolicy {
            max_retries: 3,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            jitter_ms: 5,
        };

        let attempts = AtomicUsize::new(0);

        // Classifier that returns different types based on attempt number
        let classifier = |err: &usize| match *err {
            0 => RetryErrorType::Retryable,
            1 => RetryErrorType::Throttled(Duration::from_millis(10)),
            _ => RetryErrorType::Retryable,
        };

        let result = retry_with_backoff(runtime, policy, classifier, "test_operation", || {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt < 2 {
                    Err(attempt) // Return attempt number as error
                } else {
                    Ok("success") // Succeed on third attempt
                }
            })
        })
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Should try three times
    }
}
