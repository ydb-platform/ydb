GO_LIBRARY()

SRCS(
    header.go
    pipeline.go
    reader.go
    textproto.go
    writer.go
)

GO_TEST_SRCS(
    header_test.go
    reader_test.go
    writer_test.go
)

END()

RECURSE(
)
