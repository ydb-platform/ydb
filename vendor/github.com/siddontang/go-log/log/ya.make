GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    filehandler.go
    handler.go
    log.go
    logger.go
)

GO_TEST_SRCS(log_test.go)

END()

RECURSE(gotest)
