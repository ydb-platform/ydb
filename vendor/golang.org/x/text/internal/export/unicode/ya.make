GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(doc.go)

GO_TEST_SRCS(unicode_test.go)

END()

RECURSE(gotest)
