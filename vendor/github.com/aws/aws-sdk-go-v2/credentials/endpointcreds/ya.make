GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    provider.go
)

GO_XTEST_SRCS(provider_test.go)

END()

RECURSE(
    gotest
    internal
)
