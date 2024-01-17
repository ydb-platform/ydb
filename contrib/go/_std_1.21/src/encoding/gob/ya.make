GO_LIBRARY()

SRCS(
    dec_helpers.go
    decode.go
    decoder.go
    doc.go
    enc_helpers.go
    encode.go
    encoder.go
    error.go
    type.go
)

GO_TEST_SRCS(
    codec_test.go
    encoder_test.go
    gobencdec_test.go
    timing_test.go
    type_test.go
)

GO_XTEST_SRCS(
    example_encdec_test.go
    example_interface_test.go
    example_test.go
)

END()

RECURSE(
)
