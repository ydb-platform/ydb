GO_LIBRARY()

SRCS(
    decode.go
    encode.go
    fold.go
    indent.go
    scanner.go
    stream.go
    tables.go
    tags.go
)

GO_TEST_SRCS(
    bench_test.go
    decode_test.go
    encode_test.go
    fold_test.go
    fuzz_test.go
    number_test.go
    scanner_test.go
    stream_test.go
    tagkey_test.go
    tags_test.go
)

GO_XTEST_SRCS(
    example_marshaling_test.go
    example_test.go
    example_text_marshaling_test.go
)

END()

RECURSE(
)
