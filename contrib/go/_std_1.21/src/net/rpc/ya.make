GO_LIBRARY()

SRCS(
    client.go
    debug.go
    server.go
)

GO_TEST_SRCS(
    client_test.go
    server_test.go
)

END()

RECURSE(
    jsonrpc
)
