GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    middleware.go
)

GO_TEST_SRCS(client_test.go)

END()

RECURSE(
    gotest
)
