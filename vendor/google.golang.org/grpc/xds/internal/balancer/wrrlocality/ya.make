GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    logging.go
)

GO_TEST_SRCS(balancer_test.go)

END()

RECURSE(gotest)
