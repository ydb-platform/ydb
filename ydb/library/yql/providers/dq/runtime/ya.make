LIBRARY()

PEERDIR(
    yql/essentials/minikql/invoke_builtins
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/utils/backtrace
    yql/essentials/core/expr_nodes
    ydb/library/yql/dq/common
    yql/essentials/core/dq_integration/transform
    ydb/library/yql/dq/runtime
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/counters
    ydb/library/yql/providers/dq/task_runner
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/parser/pg_catalog
)

YQL_LAST_ABI_VERSION()

SRCS(
    file_cache.cpp
    task_command_executor.cpp
    runtime_data.cpp
)

END()

RECURSE_FOR_TESTS(ut)
