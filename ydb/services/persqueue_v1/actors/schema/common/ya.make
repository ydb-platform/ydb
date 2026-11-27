LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/persqueue/public
)

SRCS(
    grpc_proxy_actor.cpp
)

END()
