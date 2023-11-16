GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    decode.go
    doc.go
    encode.go
)

GO_TEST_SRCS(
    all_test.go
    decode_test.go
    encode_test.go
)

END()

RECURSE(gotest)
