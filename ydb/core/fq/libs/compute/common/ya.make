LIBRARY()

SRCS(
    pinger.cpp
    plan2svg.cpp
    run_actor_params.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/json/yson
    ydb/core/fq/libs/config/protos
    # ydb/core/fq/libs/control_plane_storage/internal
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/shared_resources
    ydb/library/yql/public/issue
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/s3/actors_factory
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
