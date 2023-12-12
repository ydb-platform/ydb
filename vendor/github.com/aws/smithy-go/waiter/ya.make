GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    logger.go
    waiter.go
)

GO_TEST_SRCS(waiter_test.go)

END()

RECURSE(
    gotest
)
