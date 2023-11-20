GO_LIBRARY()

LICENSE(MIT)

SRCS(
    decimal-go.go
    decimal.go
    rounding.go
)

GO_TEST_SRCS(
    decimal_bench_test.go
    decimal_test.go
)

END()

RECURSE(gotest)
