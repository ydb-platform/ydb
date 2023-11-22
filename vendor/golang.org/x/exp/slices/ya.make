GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    cmp.go
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

END()

RECURSE(
    gotest
)
