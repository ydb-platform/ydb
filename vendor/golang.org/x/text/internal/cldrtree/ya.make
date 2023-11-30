GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    cldrtree.go
    generate.go
    option.go
    tree.go
    type.go
)

GO_TEST_SRCS(cldrtree_test.go)

END()

RECURSE(
    gotest
)
