GO_LIBRARY()

SRCS(
    binary.go
    native_endian_little.go
    varint.go
)

GO_TEST_SRCS(
    binary_test.go
    varint_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
