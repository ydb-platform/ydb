GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    config.go
    logging.go
    scheduler.go
    weightedroundrobin.go
)

GO_TEST_SRCS(weightedroundrobin_test.go)

GO_XTEST_SRCS(balancer_test.go)

END()

RECURSE(
    gotest
    internal
)
