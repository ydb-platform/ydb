GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    doc.go
    provider.go
)

GO_TEST_SRCS(provider_test.go)

END()

RECURSE(
    gotest
)
