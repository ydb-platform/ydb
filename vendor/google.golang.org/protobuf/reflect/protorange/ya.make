GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(range.go)

GO_TEST_SRCS(range_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(gotest)
