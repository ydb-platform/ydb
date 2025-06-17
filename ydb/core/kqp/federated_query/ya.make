LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
    kqp_federated_query_helpers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/grpc
    ydb/library/db_pool/protos
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/s3/actors_factory
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/yt/comp_nodes/dq
    ydb/library/yql/providers/yt/gateway/native
    ydb/library/yql/providers/yt/lib/yt_download
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
