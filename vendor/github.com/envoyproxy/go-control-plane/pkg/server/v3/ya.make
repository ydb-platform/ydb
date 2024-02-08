GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    gateway.go
    server.go
)

GO_XTEST_SRCS(
    delta_test.go
    gateway_test.go
    server_test.go
)

END()

RECURSE(
    gotest
)
