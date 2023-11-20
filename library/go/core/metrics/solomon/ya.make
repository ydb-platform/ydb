GO_LIBRARY()

SRCS(
    converter.go
    counter.go
    func_counter.go
    func_gauge.go
    func_int_gauge.go
    gauge.go
    histogram.go
    int_gauge.go
    metrics.go
    metrics_opts.go
    registry.go
    registry_opts.go
    spack.go
    spack_compression.go
    stream.go
    timer.go
    vec.go
)

GO_TEST_SRCS(
    converter_test.go
    counter_test.go
    func_counter_test.go
    func_gauge_test.go
    func_int_gauge_test.go
    gauge_test.go
    histogram_test.go
    int_gauge_test.go
    metrics_test.go
    race_test.go
    registry_test.go
    spack_compression_test.go
    spack_test.go
    stream_test.go
    timer_test.go
    vec_test.go
)

END()

RECURSE(
    gotest
)
