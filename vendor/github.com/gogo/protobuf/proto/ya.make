GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.3.2)

SRCS(
    clone.go
    custom_gogo.go
    decode.go
    deprecated.go
    discard.go
    duration.go
    duration_gogo.go
    encode.go
    encode_gogo.go
    equal.go
    extensions.go
    extensions_gogo.go
    lib.go
    lib_gogo.go
    message_set.go
    pointer_unsafe.go
    pointer_unsafe_gogo.go
    properties.go
    properties_gogo.go
    skip_gogo.go
    table_marshal.go
    table_marshal_gogo.go
    table_merge.go
    table_unmarshal.go
    table_unmarshal_gogo.go
    text.go
    text_gogo.go
    text_parser.go
    timestamp.go
    timestamp_gogo.go
    wrappers.go
    wrappers_gogo.go
)

GO_TEST_SRCS(
    # size2_test.go
)

GO_XTEST_SRCS(
    # all_test.go
    # any_test.go
    # clone_test.go
    # decode_test.go
    # discard_test.go
    # encode_test.go
    # equal_test.go
    # extensions_test.go
    # map_test.go
    # message_set_test.go
    # proto3_test.go
    # size_test.go
    # text_parser_test.go
    # text_test.go
)

END()

RECURSE(
    gotest
    proto3_proto
    test_proto
)
