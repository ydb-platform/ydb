LIBRARY()

SRCS(
    eds.cpp
    grpc_service.cpp
)

PEERDIR(
    library/cpp/digest/md5
    ydb/core/discovery
    ydb/library/actors/async
    ydb/library/grpc/server
    ydb/public/api/grpc/draft/xds
    ydb/public/api/grpc/draft/xds/codec
    ydb/core/grpc_services
    ydb/core/mind
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/library/operation_id
)

END()
