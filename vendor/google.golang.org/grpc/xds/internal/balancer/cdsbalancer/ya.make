GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    cdsbalancer.go
    cluster_handler.go
    logging.go
)

GO_TEST_SRCS(
    cdsbalancer_security_test.go
    cdsbalancer_test.go
    cluster_handler_test.go
)

END()

RECURSE(gotest)
