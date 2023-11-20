GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array_codec.go
    bsoncodec.go
    byte_slice_codec.go
    cond_addr_codec.go
    default_value_decoders.go
    default_value_encoders.go
    doc.go
    empty_interface_codec.go
    map_codec.go
    mode.go
    pointer_codec.go
    proxy.go
    registry.go
    slice_codec.go
    string_codec.go
    struct_codec.go
    struct_tag_parser.go
    time_codec.go
    types.go
    uint_codec.go
)

GO_TEST_SRCS(
    bsoncodec_test.go
    cond_addr_codec_test.go
    default_value_decoders_test.go
    default_value_encoders_test.go
    registry_test.go
    string_codec_test.go
    struct_codec_test.go
    struct_tag_parser_test.go
    time_codec_test.go
)

GO_XTEST_SRCS(registry_examples_test.go)

END()

RECURSE(gotest)
