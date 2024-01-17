GO_LIBRARY()

SRCS(
    compile.go
    doc.go
    op_string.go
    parse.go
    perl_groups.go
    prog.go
    regexp.go
    simplify.go
)

GO_TEST_SRCS(
    parse_test.go
    prog_test.go
    simplify_test.go
)

END()

RECURSE(
)
