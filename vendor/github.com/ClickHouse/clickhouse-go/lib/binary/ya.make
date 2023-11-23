GO_LIBRARY()

LICENSE(MIT)

SRCS(
    compress_reader.go
    compress_settings.go
    compress_writer.go
    decoder.go
    encoder.go
)

GO_TEST_SRCS(
    binary_benchmark_test.go
    binary_test.go
    compress_test.go
)

END()

RECURSE(gotest)
