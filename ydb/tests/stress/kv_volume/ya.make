PROGRAM(workload_keyvalue_volume)

SRCS(
    main.cpp
    utils.cpp
    run_stats.cpp
    data_storage.cpp
    keyvalue_client.cpp
    keyvalue_client_v1.cpp
    keyvalue_client_v2.cpp
    worker.cpp
)

PEERDIR(
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
