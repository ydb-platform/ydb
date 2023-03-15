LIBRARY()

SRCS(
    empty_gateway.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/yq/libs/actors
    ydb/core/yq/libs/events
    ydb/core/yq/libs/read_rule
    ydb/core/yq/libs/shared_resources
    ydb/core/yq/libs/tasks_packer
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/public/issue
    ydb/public/api/protos
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/proto
)

YQL_LAST_ABI_VERSION()

END()
