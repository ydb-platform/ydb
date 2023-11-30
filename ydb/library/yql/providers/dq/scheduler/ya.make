LIBRARY()

PEERDIR(
    ydb/library/yql/dq/common
    ydb/library/yql/dq/proto
    ydb/library/yql/providers/dq/config
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/actors/protos
)

SRCS(
    dq_scheduler.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
