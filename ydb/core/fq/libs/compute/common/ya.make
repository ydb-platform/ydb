LIBRARY()

SRCS(
    pinger.cpp
    run_actor_params.cpp
    utils.cpp
)

PEERDIR(
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/shared_resources
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
