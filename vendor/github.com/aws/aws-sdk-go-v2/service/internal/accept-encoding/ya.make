GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    accept_encoding_gzip.go
    doc.go
    go_module_metadata.go
)

GO_TEST_SRCS(accept_encoding_gzip_test.go)

END()

RECURSE(
    gotest
)
