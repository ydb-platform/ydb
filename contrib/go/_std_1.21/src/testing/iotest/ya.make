GO_LIBRARY()

SRCS(
    logger.go
    reader.go
    writer.go
)

GO_TEST_SRCS(
    logger_test.go
    reader_test.go
    writer_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
