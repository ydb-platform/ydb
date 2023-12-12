GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    decoder_util.go
)

GO_TEST_SRCS(decoder_util_test.go)

END()

RECURSE(
    gotest
)
