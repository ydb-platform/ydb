GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    config.go
    context.go
    credential_cache.go
    credentials.go
    defaultsmode.go
    doc.go
    endpoints.go
    errors.go
    from_ptr.go
    go_module_metadata.go
    logging.go
    request.go
    retryer.go
    runtime.go
    to_ptr.go
    types.go
    version.go
)

GO_TEST_SRCS(
    credential_cache_bench_test.go
    credential_cache_test.go
    credentials_test.go
    endpoints_test.go
    retryer_test.go
)

END()

RECURSE(
    arn
    defaults
    gotest
    middleware
    protocol
    ratelimit
    retry
    signer
    transport
)
