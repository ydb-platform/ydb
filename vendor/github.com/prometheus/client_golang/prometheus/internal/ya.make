GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    almost_equal.go
    difflib.go
    go_collector_options.go
    go_runtime_metrics.go
    metric.go
)

GO_TEST_SRCS(
    difflib_test.go
    go_runtime_metrics_test.go
)

END()

RECURSE(gotest)
