LIBRARY()

SRCS(
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/pq/cm_client
    ydb/library/yql/providers/pq/provider
    ydb/library/yql/utils
    ydb/public/sdk/cpp/client/ydb_datastreams
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_topic
)

END()
