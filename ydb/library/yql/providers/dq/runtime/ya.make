LIBRARY()

PEERDIR(
    ydb/library/yql/dq/common
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/task_runner
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/utils
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

SRCS(
    runtime_data.cpp
)

END()

RECURSE_FOR_TESTS(ut)
