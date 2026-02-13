PROGRAM(workload_keyvalue_volume)

SRCS(
    main.cpp
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
