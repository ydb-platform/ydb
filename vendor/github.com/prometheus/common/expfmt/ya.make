GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    decode.go
    encode.go
    expfmt.go
    openmetrics_create.go
    text_create.go
    text_parse.go
)

GO_TEST_SRCS(
    bench_test.go
    decode_test.go
    encode_test.go
    openmetrics_create_test.go
    text_create_test.go
    text_parse_test.go
)

END()

RECURSE(gotest)
