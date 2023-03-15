LIBRARY()

SRCS(
    grpc_service.cpp
    private_grpc.cpp
)

PEERDIR(
    library/cpp/grpc/server
    library/cpp/retry
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/yq/libs/grpc
    ydb/library/protobuf_printer
)

END()

RECURSE_FOR_TESTS(
    ut_integration
)
