UNITTEST_FOR(ydb/library/yql/dq/actors/compute)

IF (NOT OS_WINDOWS)
SRCS(
    dq_async_compute_actor_ut.cpp
    dq_sync_compute_actor_ut.cpp
)
ELSE()
# TTestActorRuntimeBase(..., true) seems broken on windows
ENDIF()

SRCS(
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_actor_channels_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
    mock_lookup_factory.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/actors/testlib
    ydb/library/actors/wilson
    ydb/library/services
    ydb/library/testlib/common
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/actors/compute/ut/proto
    ydb/library/yql/dq/actors/input_transforms
    ydb/library/yql/dq/actors/task_runner
    ydb/library/yql/dq/comp_nodes/no_llvm
    ydb/library/yql/dq/tasks
    ydb/library/yql/dq/transform
    ydb/library/yql/providers/dq/task_runner
    ydb/library/yql/public/ydb_issue
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/providers/common/comp_nodes
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SIZE(MEDIUM)

END()

RECURSE(
   proto
)
