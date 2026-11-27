LIBRARY()

SRCS(
    ymq_proxy.cpp
    grpc_service.cpp
    utils.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/protos
    ydb/core/tx/scheme_board
    ydb/core/ymq/actor
    ydb/core/ymq/base
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/library/http_proxy/error
    ydb/library/services
    ydb/public/api/grpc/draft
    yql/essentials/public/issue
)

END()
