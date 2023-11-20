GO_LIBRARY()

LICENSE(MIT)

SRCS(
    logger.go
    scoped.go
)

GO_XTEST_SRCS(logging_test.go)

END()

RECURSE(gotest)
