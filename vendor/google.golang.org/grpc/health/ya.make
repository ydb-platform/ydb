GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    logging.go
    server.go
)

GO_TEST_SRCS(
    client_test.go
    server_internal_test.go
)

GO_XTEST_SRCS(server_test.go)

END()

RECURSE(
    gotest
    grpc_health_v1
    # yo
)
