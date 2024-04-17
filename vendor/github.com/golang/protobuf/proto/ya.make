GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    buffer.go
    defaults.go
    deprecated.go
    discard.go
    extensions.go
    properties.go
    proto.go
    registry.go
    text_decode.go
    text_encode.go
    wire.go
    wrappers.go
)

GO_XTEST_SRCS(
    # discard_test.go
    # extensions_test.go
    proto_clone_test.go
    proto_equal_test.go
    proto_test.go
    registry_test.go
    # text_test.go
)

END()

RECURSE(
    gotest
)
