GO_LIBRARY()

SRCS(
    lex.go
    node.go
    parse.go
)

GO_TEST_SRCS(
    lex_test.go
    parse_test.go
)

END()

RECURSE(
)
