GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.2.0)

# Those tests do open ./fixtures, so we have to mess up with cwd a bit

DATA(
    arcadia/vendor/github.com/pseudomuto/protokit/
)

TEST_CWD(vendor/github.com/pseudomuto/protokit/)

SRCS(
    comments.go
    context.go
    doc.go
    parser.go
    plugin.go
    types.go
    version.go
)

GO_XTEST_SRCS(
    comments_test.go
    context_test.go
    example_plugin_test.go
    parser_bench_test.go
    parser_test.go
    # plugin_test.go
)

END()

RECURSE(
    gotest
    utils
)
