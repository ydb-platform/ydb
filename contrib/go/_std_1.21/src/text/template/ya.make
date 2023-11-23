GO_LIBRARY()

SRCS(
    doc.go
    exec.go
    funcs.go
    helper.go
    option.go
    template.go
)

GO_TEST_SRCS(
    exec_test.go
    multi_test.go
)

GO_XTEST_SRCS(
    example_test.go
    examplefiles_test.go
    examplefunc_test.go
    link_test.go
)

END()

RECURSE(
    parse
)
