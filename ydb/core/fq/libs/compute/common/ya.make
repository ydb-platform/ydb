LIBRARY()

SRCS(
    pinger.cpp
    run_actor_params.cpp
)

PEERDIR(
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/shared_resources
    ydb/library/yql/providers/dq/provider
)

YQL_LAST_ABI_VERSION()

END()
