GO_LIBRARY()

LICENSE(MIT)

SRCS(
    array.go
    column.go
    common.go
    date.go
    datetime.go
    datetime64.go
    decimal.go
    enum.go
    fixed_string.go
    float32.go
    float64.go
    int16.go
    int32.go
    int64.go
    int8.go
    ip.go
    ipv4.go
    ipv6.go
    nullable.go
    nullable_appender.go
    string.go
    tuple.go
    uint16.go
    uint32.go
    uint64.go
    uint8.go
    uuid.go
)

GO_TEST_SRCS(
    column_benchmark_test.go
    decimal_test.go
    ip_test.go
    uuid_test.go
)

GO_XTEST_SRCS(column_test.go)

END()

RECURSE(gotest)
