GO_LIBRARY()

LICENSE(BSD-2-Clause)

SRCS(
    cluster.go
    cluster_commands.go
    command.go
    commands.go
    doc.go
    error.go
    iterator.go
    options.go
    pipeline.go
    pubsub.go
    redis.go
    result.go
    ring.go
    script.go
    sentinel.go
    tx.go
    universal.go
    version.go
)

GO_TEST_SRCS(
    bench_decode_test.go
    export_test.go
    internal_test.go
    options_test.go
)

GO_XTEST_SRCS(
    bench_test.go
    cluster_test.go
    command_test.go
    commands_test.go
    example_instrumentation_test.go
    example_test.go
    iterator_test.go
    main_test.go
    pipeline_test.go
    pool_test.go
    pubsub_test.go
    race_test.go
    redis_test.go
    ring_test.go
    sentinel_test.go
    tx_test.go
    universal_test.go
)

END()

RECURSE(
    # gotest
    internal
)
