GO_LIBRARY()

SRCS(
    slices.go
    sort.go
    zsortanyfunc.go
    zsortordered.go
)

GO_TEST_SRCS(
    slices_test.go
    sort_benchmark_test.go
    sort_test.go
)

GO_XTEST_SRCS(example_test.go)

END()

RECURSE(
)
