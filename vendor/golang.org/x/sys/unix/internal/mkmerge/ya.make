GO_PROGRAM()

LICENSE(BSD-3-Clause)

SRCS(
    mkmerge.go
)

GO_TEST_SRCS(mkmerge_test.go)

END()

RECURSE(
    gotest
)
