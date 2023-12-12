GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cache.go
    const.go
    header_rules.go
    headers.go
    hmac.go
    host.go
    scope.go
    time.go
    util.go
)

GO_TEST_SRCS(
    headers_test.go
    util_test.go
)

END()

RECURSE(
    gotest
)
