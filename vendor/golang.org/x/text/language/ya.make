GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    coverage.go
    doc.go
    language.go
    match.go
    parse.go
    tables.go
    tags.go
)

GO_TEST_SRCS(
    coverage_test.go
    language_test.go
    lookup_test.go
    match_test.go
    parse_test.go
)

GO_XTEST_SRCS(
    examples_test.go
    httpexample_test.go
)

END()

RECURSE(
    display
    gotest
)
