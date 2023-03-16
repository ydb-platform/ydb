LIBRARY()

SRCS(
    read_rule_creator.cpp
    read_rule_deleter.cpp
)

PEERDIR(
    library/cpp/actors/core
    ydb/core/fq/libs/events
    ydb/core/protos
    ydb/public/api/protos
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/ydb_persqueue_core
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

END()
