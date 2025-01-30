LIBRARY()

SRCS(
    read_rule_creator.cpp
    read_rule_deleter.cpp
)

PEERDIR(
    ydb/core/fq/libs/events
    ydb/core/protos
    ydb/library/actors/core
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/library/operation_id
    ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()
