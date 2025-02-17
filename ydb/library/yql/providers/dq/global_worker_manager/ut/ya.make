UNITTEST_FOR(ydb/library/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SIZE(SMALL)

PEERDIR(
    ydb/library/actors/testlib
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/library/yql/providers/dq/actors/yt
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation/llvm14
    yql/essentials/minikql/comp_nodes/llvm14

    yql/essentials/core/dq_integration/transform
    ydb/library/yql/dq/comp_nodes
    yql/essentials/providers/common/comp_nodes
    yql/essentials/minikql/comp_nodes
    ydb/library/yql/dq/transform
    ydb/library/yql/providers/dq/task_runner    
)

SRCS(
    global_worker_manager_ut.cpp
    workers_storage_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

YQL_LAST_ABI_VERSION()

END()
