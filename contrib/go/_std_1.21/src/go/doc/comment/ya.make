GO_LIBRARY()

SRCS(
    doc.go
    html.go
    markdown.go
    parse.go
    print.go
    std.go
    text.go
)

GO_TEST_SRCS(
    old_test.go
    parse_test.go
    std_test.go
    testdata_test.go
    wrap_test.go
)

END()

RECURSE(
)
