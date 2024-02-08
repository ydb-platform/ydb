GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    callcounter.go
    config.go
    logging.go
    subconn_wrapper.go
)

GO_TEST_SRCS(
    balancer_test.go
    callcounter_test.go
    config_test.go
)

END()

RECURSE(
    e2e_test
    gotest
)
