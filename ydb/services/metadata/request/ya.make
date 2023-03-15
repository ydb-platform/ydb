LIBRARY()

SRCS(
    request_actor.cpp
    config.cpp
    common.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
)

END()
