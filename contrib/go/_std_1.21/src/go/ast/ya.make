GO_LIBRARY()

SRCS(
    ast.go
    commentmap.go
    filter.go
    import.go
    print.go
    resolve.go
    scope.go
    walk.go
)

GO_TEST_SRCS(
    ast_test.go
    print_test.go
)

GO_XTEST_SRCS(
    commentmap_test.go
    example_test.go
    filter_test.go
    issues_test.go
)

END()

RECURSE(
)
