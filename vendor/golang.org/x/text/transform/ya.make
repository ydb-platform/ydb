GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(transform.go)

GO_TEST_SRCS(transform_test.go)

GO_XTEST_SRCS(examples_test.go)

END()

RECURSE(
    # gotest
)
