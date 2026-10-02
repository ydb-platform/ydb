LIBRARY()

SRCS(
    yql_pq_gateway_factory.cpp
    yql_pq_gateway_services.cpp
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/pq/cm_client
    ydb/library/yql/providers/pq/gateway/abstract
    ydb/library/yql/providers/pq/gateway/clients/external
    ydb/library/yql/providers/pq/gateway/clients/local
    ydb/library/yverify_stream
    ydb/public/sdk/cpp/src/client/datastreams
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yql/essentials/utils/log
)

END()
