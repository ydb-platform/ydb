GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    decode.go
    doc.go
    encode.go
    well_known_types.go
)

GO_XTEST_SRCS(
    bench_test.go
    decode_test.go
    encode_test.go
)

END()

RECURSE(gotest)
