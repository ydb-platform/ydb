GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    client.go
    content_type.go
    response_error.go
    response_error_middleware.go
    timeout_read_closer.go
)

GO_TEST_SRCS(
    client_test.go
    timeout_read_closer_test.go
)

END()

RECURSE(
    gotest
)
