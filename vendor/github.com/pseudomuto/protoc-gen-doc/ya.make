GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.5.1)

# Those tests do open ./fixtures, so we have to mess up with cwd a bit

DATA(
    arcadia/vendor/github.com/pseudomuto/protoc-gen-doc/
)

TEST_CWD(vendor/github.com/pseudomuto/protoc-gen-doc/)

SRCS(
    doc.go
    filters.go
    plugin.go
    renderer.go
    resources.go
    template.go
    version.go
)

GO_XTEST_SRCS(
    bench_test.go
    filters_test.go
    plugin_test.go
    renderer_test.go
    template_test.go
)

GO_EMBED_PATTERN(resources/docbook.tmpl)

GO_EMBED_PATTERN(resources/html.tmpl)

GO_EMBED_PATTERN(resources/markdown.tmpl)

GO_EMBED_PATTERN(resources/scalars.json)

END()

RECURSE(
    cmd
    extensions
    fixtures
    gotest
    thirdparty
)
