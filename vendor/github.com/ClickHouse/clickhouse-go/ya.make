GO_LIBRARY()

LICENSE(MIT)

SRCS(
    array.go
    bootstrap.go
    clickhouse.go
    clickhouse_exception.go
    clickhouse_ping.go
    clickhouse_profile_info.go
    clickhouse_progress.go
    clickhouse_read_block.go
    clickhouse_read_meta.go
    clickhouse_send_external_data.go
    clickhouse_send_query.go
    clickhouse_write_block.go
    connect.go
    helpers.go
    query_settings.go
    result.go
    rows.go
    stmt.go
    tls_config.go
    word_matcher.go
    write_column.go
)

GO_TEST_SRCS(
    bootstrap_test.go
    clickhouse_naive_test.go
    # clickhouse_nullable_array_test.go
    # connect_check_test.go
    helpers_test.go
    # issues_test.go
    word_matcher_test.go
)

GO_XTEST_SRCS(
    # clickhouse_columnar_test.go
    # clickhouse_compress_test.go
    # clickhouse_custom_types_test.go
    # clickhouse_decimal128_test.go
    # clickhouse_decimal_test.go
    # clickhouse_direct_test.go
    # clickhouse_negative_test.go
    # clickhouse_nullable_test.go
    # clickhouse_test.go
)

IF (OS_LINUX)
    SRCS(connect_check.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(connect_check.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(connect_check_dummy.go)
ENDIF()

END()

RECURSE(
    # examples
    gotest
    lib
)
