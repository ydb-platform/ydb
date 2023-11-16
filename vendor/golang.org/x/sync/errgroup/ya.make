GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    errgroup.go
    go120.go
)

GO_XTEST_SRCS(
    errgroup_example_md5all_test.go
    errgroup_test.go
    go120_test.go
)

END()

RECURSE(
    gotest
)
