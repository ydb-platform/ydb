GO_LIBRARY()

SRCS(
    reader.go
    writer.go
)

GO_TEST_SRCS(
    reader_test.go
    writer_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
