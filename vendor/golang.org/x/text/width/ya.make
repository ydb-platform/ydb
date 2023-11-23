GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    kind_string.go
    tables15.0.0.go
    transform.go
    trieval.go
    width.go
)

GO_TEST_SRCS(
    common_test.go
    runes_test.go
    tables_test.go
    transform_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
    gotest
)
