GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(tag.go)

GO_XTEST_SRCS(tag_test.go)

END()

RECURSE(gotest)
