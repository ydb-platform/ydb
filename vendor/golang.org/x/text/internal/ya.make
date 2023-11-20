GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    internal.go
    match.go
)

GO_TEST_SRCS(
    internal_test.go
    match_test.go
)

END()

RECURSE(
    catmsg
    cldrtree
    colltab
    export
    format
    gen
    gotest
    language
    number
    stringset
    tag
    testtext
    triegen
    ucd
    utf8internal
)
