GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    adaptive.go
    adaptive_ratelimit.go
    adaptive_token_bucket.go
    doc.go
    errors.go
    jitter_backoff.go
    metadata.go
    middleware.go
    retry.go
    retryable_error.go
    standard.go
    throttle_error.go
    timeout_error.go
)

GO_TEST_SRCS(
    adaptive_ratelimit_test.go
    adaptive_test.go
    adaptive_token_bucket_test.go
    jitter_backoff_test.go
    middleware_test.go
    retry_test.go
    retryable_error_test.go
    standard_test.go
    timeout_error_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
    internal
)
