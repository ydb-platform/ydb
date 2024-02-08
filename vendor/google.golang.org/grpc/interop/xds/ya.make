GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(custom_lb.go)

GO_TEST_SRCS(custom_lb_test.go)

END()

RECURSE(
    client
    gotest
    server
)
