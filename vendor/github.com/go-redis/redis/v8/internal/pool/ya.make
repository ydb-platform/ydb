GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    conn.go
    pool.go
    pool_single.go
    pool_sticky.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    bench_test.go
    main_test.go
    pool_test.go
)

END()

RECURSE(gotest)
