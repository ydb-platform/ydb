LIBRARY()

SRCS(
    grpc_service.cpp
    private_grpc.cpp
    ydb_over_fq.cpp
)

PEERDIR(
    ydb/core/fq/libs/grpc
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/ydb_over_fq
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/library/protobuf_printer
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
)

END()
