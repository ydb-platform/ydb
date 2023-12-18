GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array.go
    constants.go
    doc.go
    element.go
    encoder.go
    error_utils.go
    escape.go
    map.go
    value.go
    xml_decoder.go
)

GO_TEST_SRCS(
    array_test.go
    error_utils_test.go
    map_test.go
    value_test.go
    xml_decoder_test.go
)

GO_XTEST_SRCS(encoder_test.go)

END()

RECURSE(
    gotest
)
