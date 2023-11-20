GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    bind.go
    clickhouse.go
    clickhouse_options.go
    clickhouse_rows.go
    clickhouse_rows_column_type.go
    clickhouse_std.go
    conn.go
    conn_async_insert.go
    conn_batch.go
    conn_exec.go
    conn_handshake.go
    conn_http.go
    conn_http_async_insert.go
    conn_http_batch.go
    conn_http_exec.go
    conn_http_query.go
    conn_logs.go
    conn_ping.go
    conn_process.go
    conn_profile_events.go
    conn_query.go
    conn_send_query.go
    context.go
    scan.go
    struct_map.go
)

GO_TEST_SRCS(
    bind_test.go
    struct_map_test.go
)

IF (OS_LINUX)
    SRCS(conn_check.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(conn_check.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(conn_check_ping.go)
ENDIF()

END()

RECURSE(
    benchmark
    contributors
    # examples
    ext
    lib
    resources
    # tests
)
