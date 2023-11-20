GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    merge.go
    rangetable.go
    tables13.0.0.go
)

GO_TEST_SRCS(
    merge_test.go
    rangetable_test.go
)

END()

RECURSE(gotest)
