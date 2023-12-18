GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    token_bucket.go
    token_rate_limit.go
)

GO_TEST_SRCS(
    token_bucket_test.go
    token_rate_limit_test.go
)

END()

RECURSE(
    gotest
)
