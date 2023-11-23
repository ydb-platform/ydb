GO_LIBRARY()

LICENSE(MIT)

SRCS(
    color.go
    decode.go
    encode.go
    error.go
    json.go
    option.go
    path.go
    query.go
)

GO_TEST_SRCS(
    export_test.go
    size_test.go
)

GO_XTEST_SRCS(
    color_test.go
    decode_test.go
    encode_test.go
    helper_test.go
    json_test.go
    number_test.go
    path_test.go
    query_test.go
    stream_test.go
    tagkey_test.go
)

END()

RECURSE(
    gotest
    internal
    test
)
