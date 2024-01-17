GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    call_metrics.go
    orca.go
    producer.go
    server_metrics.go
    service.go
)

GO_TEST_SRCS(
    #server_metrics_test.go
)

GO_XTEST_SRCS(
    call_metrics_test.go
    orca_test.go
    producer_test.go
    service_test.go
)

END()

RECURSE(
    gotest
    internal
)
