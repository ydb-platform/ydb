GO_LIBRARY()

SRCS(
    client.go
    debug.go
    server.go
)

END()

RECURSE(
    jsonrpc
)
