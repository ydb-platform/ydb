GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    resolvers.go
)

GO_TEST_SRCS(resolvers_test.go)

END()

RECURSE(
    gotest
)
