GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    arn.go
    doc.go
    host.go
    partition.go
    partitions.go
)

GO_TEST_SRCS(
    arn_test.go
    host_test.go
    partition_test.go
)

END()

RECURSE(
    gotest
)
