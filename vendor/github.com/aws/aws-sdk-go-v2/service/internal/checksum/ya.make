GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    algorithms.go
    aws_chunked_encoding.go
    go_module_metadata.go
    middleware_add.go
    middleware_compute_input_checksum.go
    middleware_setup_context.go
    middleware_validate_output.go
)

GO_TEST_SRCS(
    algorithms_test.go
    aws_chunked_encoding_test.go
    middleware_add_test.go
    middleware_compute_input_checksum_test.go
    middleware_setup_context_test.go
    middleware_validate_output_test.go
)

END()

RECURSE(
    gotest
)
