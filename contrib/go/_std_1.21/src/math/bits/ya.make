GO_LIBRARY()

SRCS(
    bits.go
    bits_errors.go
    bits_tables.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    bits_test.go
    example_math_test.go
    example_test.go
)

END()

RECURSE(
)
