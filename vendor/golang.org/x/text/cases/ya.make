GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    cases.go
    context.go
    fold.go
    info.go
    map.go
    tables15.0.0.go
    trieval.go
)

GO_TEST_SRCS(
    context_test.go
    fold_test.go
    map_test.go
    tables15.0.0_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
