LIBRARY()

SRCS(
    kqp_federated_query_helpers.cpp
    physical_graph_rescaling.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/fq/libs/credentials
    ydb/core/fq/libs/db_id_async_resolver_impl
    ydb/core/fq/libs/state
    ydb/core/kqp/query_data
    ydb/core/local_proxy/local_pq_client
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/logger
    ydb/library/yql/dq/tasks
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/comp_nodes
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/gateway/native
    ydb/library/yql/providers/pq/proto
    ydb/library/yql/providers/pq/transform
    ydb/library/yql/providers/s3/actors_factory
    ydb/library/yql/providers/s3/proto
    ydb/library/yql/providers/solomon/gateway
    ydb/library/yql/providers/yt/gateway/clients/message_stream
    ydb/public/api/protos
    ydb/public/sdk/cpp/adapters/executor
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/extensions/discovery_mutator
    yql/essentials/core/dq_integration/transform
    yql/essentials/public/issue
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/mkql_dq
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
)
