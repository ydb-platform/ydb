GO_LIBRARY()

SRCS(
    attr.go
    attr_string.go
    content.go
    context.go
    css.go
    delim_string.go
    doc.go
    element_string.go
    error.go
    escape.go
    html.go
    js.go
    jsctx_string.go
    state_string.go
    template.go
    transition.go
    url.go
    urlpart_string.go
)

GO_TEST_SRCS(
    clone_test.go
    content_test.go
    css_test.go
    escape_test.go
    exec_test.go
    html_test.go
    js_test.go
    multi_test.go
    transition_test.go
    url_test.go
)

GO_XTEST_SRCS(
    example_test.go
    examplefiles_test.go
    template_test.go
)

END()

RECURSE(
)
