GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    clusterimpl.go
    config.go
    logging.go
    picker.go
)

GO_TEST_SRCS(
    # balancer_test.go
    # config_test.go
)

END()

RECURSE(
    gotest
    tests
)
