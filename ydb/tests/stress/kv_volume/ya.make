PROGRAM(workload_keyvalue_volume)

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
ENDIF()

SRCS(
    main.cpp
    utils.cpp
    initial_load_progress.cpp
    initial_load_display.cpp
    run_stats.cpp
    worker_load.cpp
    run_display.cpp
    run_tui.cpp
    scroller.cpp
    action_pool.cpp
    grpc_async_executor.cpp
    key_bucket.cpp
    execution_context.cpp
    keyvalue_client.cpp
    keyvalue_client_v1.cpp
    keyvalue_client_v2.cpp
    worker.cpp
)

PEERDIR(
    contrib/libs/ftxui
    contrib/libs/grpc
    contrib/libs/protobuf
    library/cpp/getopt
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/tests/stress/kv_volume/protos
)

END()

RECURSE_FOR_TESTS(
    tests
)
