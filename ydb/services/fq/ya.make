LIBRARY()

SRCS(
    grpc_service.cpp
    private_grpc.cpp
    ydb_over_fq.cpp
)

PEERDIR(
    library/cpp/retry
    ydb/core/fq/libs/grpc
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/ydb_over_fq
    ydb/library/grpc/server
    ydb/library/protobuf_printer
)

END()

RECURSE_FOR_TESTS(
    ut_integration
)
