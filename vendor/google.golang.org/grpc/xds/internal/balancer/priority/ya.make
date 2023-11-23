GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    balancer.go
    balancer_child.go
    balancer_priority.go
    config.go
    ignore_resolve_now.go
    logging.go
    utils.go
)

GO_TEST_SRCS(
    balancer_test.go
    config_test.go
    ignore_resolve_now_test.go
    utils_test.go
)

END()

RECURSE(gotest)
