GO_LIBRARY()

SRCS(
    cond.go
    map.go
    mutex.go
    once.go
    oncefunc.go
    pool.go
    poolqueue.go
    runtime.go
    runtime2.go
    rwmutex.go
    waitgroup.go
)

GO_TEST_SRCS(export_test.go)

GO_XTEST_SRCS(
    cond_test.go
    example_pool_test.go
    example_test.go
    map_bench_test.go
    map_reference_test.go
    map_test.go
    mutex_test.go
    once_test.go
    oncefunc_test.go
    pool_test.go
    runtime_sema_test.go
    rwmutex_test.go
    waitgroup_test.go
)

END()

RECURSE(
    atomic
)
