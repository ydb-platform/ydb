YQL_LIBRARY()

SRCS(
    request_actor.cpp
    request_actor_cb.cpp
    config.cpp
    common.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/local_rpc
    ydb/core/protos
    ydb/library/accessor
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/public/api/protos
    yql/essentials/public/issue
)

END()
