GO_LIBRARY()

SRCS(
    kind_string.go
    value.go
)

GO_TEST_SRCS(value_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
