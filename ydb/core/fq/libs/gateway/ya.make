LIBRARY()

SRCS(
    empty_gateway.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/fq/libs/actors
    ydb/core/fq/libs/events
    ydb/core/fq/libs/read_rule
    ydb/core/fq/libs/shared_resources
    ydb/core/fq/libs/tasks_packer
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/public/issue
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/pq/proto
    ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
