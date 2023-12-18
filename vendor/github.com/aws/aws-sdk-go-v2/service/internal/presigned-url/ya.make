GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    context.go
    doc.go
    go_module_metadata.go
    middleware.go
)

GO_TEST_SRCS(middleware_test.go)

END()

RECURSE(
    gotest
)
