GO_LIBRARY()

LICENSE(MIT)

SRCS(
    batch.go
    conn.go
    copy_from.go
    doc.go
    extended_query_builder.go
    large_objects.go
    named_args.go
    rows.go
    tracer.go
    tx.go
    values.go
)

GO_XTEST_SRCS(
    # batch_test.go
    # bench_test.go
    # conn_test.go
    # copy_from_test.go
    # helper_test.go
    # large_objects_test.go
    named_args_test.go
    # pgbouncer_test.go
    # pipeline_test.go
    # query_test.go
    # rows_test.go
    # tracer_test.go
    # tx_test.go
    # values_test.go
)

END()

RECURSE(
    examples
    gotest
    internal
    log
    pgconn
    pgproto3
    pgtype
    pgxpool
    pgxtest
    stdlib
    tracelog
)
