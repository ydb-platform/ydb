GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    assert.go
    certificate_utils.go
    discard.go
    endless_reader.go
    util.go
)

GO_XTEST_SRCS(
    assert_test.go
    util_test.go
)

END()

RECURSE(
    gotest
    unit
)
