LIBRARY()

SRCS(
    dq_worker.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/protobuf/util
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface
    yt/yt/core
    ydb/library/yql/dq/actors/spilling
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/providers/common/metrics
    ydb/library/yql/providers/dq/runtime
    ydb/library/yql/providers/dq/service
    ydb/library/yql/providers/dq/stats_collector
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/public/udf/service/terminate_policy
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/utils/log/proto
    ydb/library/yql/providers/dq/actors/yt
    ydb/library/yql/providers/dq/global_worker_manager
    ydb/library/yql/utils/signals
)

YQL_LAST_ABI_VERSION()

END()
