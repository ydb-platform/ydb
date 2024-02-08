GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    compressor.go
    encode_duration.go
    grpcutil.go
    metadata.go
    method.go
    regex.go
)

GO_TEST_SRCS(
    compressor_test.go
    encode_duration_test.go
    method_test.go
    regex_test.go
)

END()

RECURSE(gotest)
