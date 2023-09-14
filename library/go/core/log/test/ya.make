GO_TEST()

TAG(ya:run_go_benchmark)

GO_TEST_SRCS(
    ctx_bench_test.go
    log_bench_test.go
    log_test.go
)

END()
