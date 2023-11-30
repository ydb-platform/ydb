GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    ucd.go
)

GO_TEST_SRCS(ucd_test.go)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
