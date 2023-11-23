GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    build_info_collector.go
    collector.go
    counter.go
    desc.go
    doc.go
    expvar_collector.go
    fnv.go
    gauge.go
    get_pid.go
    go_collector.go
    go_collector_latest.go
    histogram.go
    labels.go
    metric.go
    num_threads.go
    observer.go
    process_collector.go
    registry.go
    summary.go
    timer.go
    untyped.go
    value.go
    vec.go
    vnext.go
    wrap.go
)

GO_TEST_SRCS(
    benchmark_test.go
    collector_test.go
    counter_test.go
    desc_test.go
    gauge_test.go
    go_collector_latest_test.go
    go_collector_metrics_go120_test.go
    go_collector_test.go
    histogram_test.go
    metric_test.go
    summary_test.go
    timer_test.go
    value_test.go
    vec_test.go
    wrap_test.go
)

GO_XTEST_SRCS(
    example_clustermanager_test.go
    example_metricvec_test.go
    example_timer_complex_test.go
    example_timer_gauge_test.go
    example_timer_test.go
    examples_test.go
    expvar_collector_test.go
    registry_test.go
)

IF (OS_LINUX)
    SRCS(process_collector_other.go)

    GO_TEST_SRCS(process_collector_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(process_collector_other.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(process_collector_windows.go)

    GO_TEST_SRCS(process_collector_windows_test.go)
ENDIF()

END()

RECURSE(
    collectors
    # gotest
    graphite
    internal
    promauto
    promhttp
    push
    testutil
)
