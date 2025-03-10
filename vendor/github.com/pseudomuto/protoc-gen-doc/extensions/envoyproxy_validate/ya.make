GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.1)

SRCS(
    envoyproxy_validate.go
)

GO_XTEST_SRCS(envoyproxy_validate_test.go)

END()

RECURSE(
    gotest
)
