YQL_LIBRARY()

GENERATE_ENUM_SERIALIZATION(events.h)

GENERATE_ENUM_SERIALIZATION(event_ids.h)

PEERDIR(
    library/cpp/threading/future
    ydb/core/fq/libs/graph_params/proto
    ydb/core/fq/libs/protos
    ydb/library/actors/core
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/dq/provider
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/types/status
    yql/essentials/core
    yql/essentials/public/issue
)

END()
