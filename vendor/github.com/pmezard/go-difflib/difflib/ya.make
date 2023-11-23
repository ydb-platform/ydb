GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(difflib.go)

GO_TEST_SRCS(difflib_test.go)

END()

RECURSE(gotest)
