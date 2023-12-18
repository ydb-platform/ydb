GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    docs.go
    middleware.go
    token.go
    token_cache.go
)

GO_TEST_SRCS(
    middleware_test.go
    token_cache_test.go
)

END()

RECURSE(
    gotest
)
