GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    loadreport.go
    transport.go
)

GO_TEST_SRCS(
    # transport_test.go
)

GO_XTEST_SRCS(
    loadreport_test.go
    transport_ack_nack_test.go
    transport_backoff_test.go
    transport_new_test.go
    transport_resource_test.go
)

END()

RECURSE(gotest)
