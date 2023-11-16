GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    common.go
    compact.go
    compose.go
    coverage.go
    language.go
    lookup.go
    match.go
    parse.go
    tables.go
    tags.go
)

GO_TEST_SRCS(
    compose_test.go
    language_test.go
    lookup_test.go
    match_test.go
    parse_test.go
)

END()

RECURSE(
    compact
    gotest
)
