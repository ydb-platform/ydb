LIBRARY()

OWNER(
    galaxycrab
    g:yq
    g:yql
)

SRCS(
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/token_accessor/client 
    ydb/library/yql/utils 
    ydb/public/sdk/cpp/client/ydb_driver 
    ydb/public/sdk/cpp/client/ydb_persqueue_core 
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/pq/cm_client/interface
    ydb/library/yql/providers/pq/provider
)

END()
