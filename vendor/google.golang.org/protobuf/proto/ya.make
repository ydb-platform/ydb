GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    checkinit.go
    decode.go
    decode_gen.go
    doc.go
    encode.go
    encode_gen.go
    equal.go
    extension.go
    merge.go
    messageset.go
    proto.go
    proto_methods.go
    reset.go
    size.go
    size_gen.go
    wrappers.go
)

GO_XTEST_SRCS(
    bench_test.go
    checkinit_test.go
    decode_test.go
    encode_test.go
    equal_test.go
    extension_test.go
    merge_test.go
    messageset_test.go
    methods_test.go
    nil_test.go
    noenforceutf8_test.go
    reset_test.go
    testmessages_test.go
    validate_test.go
    weak_test.go
)

END()

RECURSE(gotest)
