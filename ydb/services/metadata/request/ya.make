LIBRARY()

SRCS(
    request_actor.cpp
    request_actor_cb.cpp
    config.cpp
    common.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/grpc_services/local_rpc
    ydb/core/grpc_services/base
    ydb/core/grpc_services
)

END()
