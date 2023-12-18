GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    encode.go
    header.go
    path_replace.go
    query.go
    uri.go
)

GO_TEST_SRCS(
    encode_test.go
    header_test.go
    path_replace_test.go
    query_test.go
    shared_test.go
    uri_test.go
)

END()

RECURSE(
    gotest
)
