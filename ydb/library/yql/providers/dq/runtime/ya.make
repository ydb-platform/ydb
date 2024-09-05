LIBRARY()

PEERDIR(
    ydb/library/yql/minikql/invoke_builtins
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/utils/backtrace
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/dq/common
    ydb/library/yql/dq/integration/transform
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/parser/pg_wrapper/interface
    ydb/library/yql/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    task_command_executor.cpp
    runtime_data.cpp
)

END()

RECURSE_FOR_TESTS(ut)
