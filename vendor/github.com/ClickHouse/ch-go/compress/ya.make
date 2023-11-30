GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    compress.go
    method_enum.go
    reader.go
    writer.go
)

GO_TEST_SRCS(
    compress_test.go
    fuzz_test.go
    reader_test.go
)

END()

RECURSE(
    gotest
)
