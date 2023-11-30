LIBRARY()

GENERATE_ENUM_SERIALIZATION(events.h)

GENERATE_ENUM_SERIALIZATION(event_ids.h)

PEERDIR(
    ydb/library/actors/core
    ydb/core/fq/libs/graph_params/proto
    ydb/core/fq/libs/protos
    ydb/library/yql/core/facade
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/dq/provider
    ydb/library/yql/public/issue
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/ydb_table
)

YQL_LAST_ABI_VERSION()

END()
