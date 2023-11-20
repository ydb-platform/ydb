GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    compact.go
    language.go
    parents.go
    tables.go
    tags.go
)

GO_TEST_SRCS(
    gen_test.go
    language_test.go
    parse_test.go
)

END()

RECURSE(gotest)
