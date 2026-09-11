LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
    streaming_query_nodes_manager.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/base
    ydb/core/mind
    ydb/core/protos
    ydb/core/util
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/pq/common
    ydb/library/yql/providers/pq/proto
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    lookup_actor
)
