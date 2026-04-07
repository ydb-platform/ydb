LIBRARY()

SRCS(
    pinger.cpp
    run_actor_params.cpp
    utils.cpp
)

PEERDIR(
    library/cpp/json/yson
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/shared_resources
    ydb/core/kqp/proxy_service/script_executions_utils
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/s3/actors_factory
    ydb/public/lib/ydb_cli/common
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
