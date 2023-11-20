GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    component.go
    grpclog.go
    logger.go
    loggerv2.go
)

GO_TEST_SRCS(loggerv2_test.go)

END()

RECURSE(
    glogger
    gotest
    # yo
)
