GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    array.go
    constants.go
    decoder_util.go
    encoder.go
    escape.go
    object.go
    value.go
)

GO_TEST_SRCS(
    array_test.go
    decoder_util_test.go
    escape_test.go
    object_test.go
    value_test.go
)

GO_XTEST_SRCS(encoder_test.go)

END()

RECURSE(
    gotest
)
