UNITTEST_FOR(ydb/library/yql/dq/actors/compute)

SRCS(
    dq_compute_actor_ut.cpp
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
    dq_async_compute_actor_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/public/ydb_issue
    ydb/library/yql/dq/actors
    ydb/library/actors/wilson
    ydb/library/actors/testlib
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/minikql/comp_nodes/no_llvm
#
    ydb/library/yql/dq/actors/task_runner
    ydb/library/yql/providers/dq/task_runner
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/computation
    yql/essentials/minikql/comp_nodes
    ydb/library/yql/dq/comp_nodes
    yql/essentials/providers/common/comp_nodes
    ydb/library/yql/dq/transform
    ydb/library/yql/dq/tasks
)

YQL_LAST_ABI_VERSION()

END()
