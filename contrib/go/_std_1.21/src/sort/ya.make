GO_LIBRARY()

SRCS(
    search.go
    slice.go
    sort.go
    zsortfunc.go
    zsortinterface.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    example_interface_test.go
    example_keys_test.go
    example_multi_test.go
    example_search_test.go
    example_test.go
    example_wrapper_test.go
    search_test.go
    sort_test.go
)

END()

RECURSE(
)
