GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    logger.go
)

GO_XTEST_SRCS(logger_test.go)

END()

RECURSE(
    gotest
)
