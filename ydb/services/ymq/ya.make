LIBRARY()

SRCS(
    ymq_proxy.cpp
    grpc_service.cpp
)

PEERDIR(
    ydb/library/grpc/server
    ydb/core/base
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/mind
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/lib/operation_id
    ydb/public/sdk/cpp/client/resources
    ydb/public/sdk/cpp/client/ydb_ymq
    ydb/services/lib/actors
    ydb/services/lib/sharding
    ydb/services/persqueue_v1
    ydb/services/ydb
)

END()
