GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    conn_wrapper.go
    listener_wrapper.go
    rds_handler.go
)

GO_TEST_SRCS(
    listener_wrapper_test.go
    rds_handler_test.go
)

END()

RECURSE(gotest)
