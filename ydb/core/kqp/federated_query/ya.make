LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
    kqp_federated_query_helpers.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/fq/libs/grpc
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/protos
    ydb/library/db_pool/protos
    ydb/library/logger
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/s3/actors_factory
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/pq/gateway/native
    yql/essentials/core/dq_integration/transform
    yql/essentials/public/issue
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/mkql_dq
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
