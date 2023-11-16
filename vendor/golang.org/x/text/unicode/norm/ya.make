GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    composition.go
    forminfo.go
    input.go
    iter.go
    normalize.go
    readwriter.go
    tables13.0.0.go
    transform.go
    trie.go
)

GO_TEST_SRCS(
    composition_test.go
    data13.0.0_test.go
    iter_test.go
    normalize_test.go
    readwriter_test.go
    transform_test.go
    ucd_test.go
)

GO_XTEST_SRCS(
    example_iter_test.go
    example_test.go
)

END()

RECURSE(gotest)
