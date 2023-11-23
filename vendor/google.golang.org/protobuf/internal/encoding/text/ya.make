GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    decode.go
    decode_number.go
    decode_string.go
    decode_token.go
    doc.go
    encode.go
)

GO_XTEST_SRCS(
    decode_test.go
    encode_test.go
)

END()

RECURSE(gotest)
