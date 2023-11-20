GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    decode.go
    doc.go
    encode.go
)

GO_XTEST_SRCS(
    decode_test.go
    encode_test.go
    other_test.go
)

END()

RECURSE(gotest)
