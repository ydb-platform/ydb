YQL_LIBRARY()

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/library/actors/core
    ydb/library/actors/interconnect
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/dq/actors/task_runner
    ydb/library/yql/dq/common
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/task_runner
    yql/essentials/minikql
    yql/essentials/utils/log
)

SRCS(
    events.cpp
    counters.cpp
)

END()
