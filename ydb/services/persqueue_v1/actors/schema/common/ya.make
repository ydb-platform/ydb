LIBRARY()

PEERDIR(
    ydb/core/grpc_services
    ydb/core/persqueue/public
)

SRCS(
    grpc_proxy_actor.cpp
)

END()
