GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    server.go
    watches.go
)

GO_TEST_SRCS(watches_test.go)

END()

RECURSE(gotest)
