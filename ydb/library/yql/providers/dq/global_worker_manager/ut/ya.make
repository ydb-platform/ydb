UNITTEST_FOR(ydb/library/yql/providers/dq/global_worker_manager)

TAG(ya:manual)

NO_BUILD_IF(OS_WINDOWS)

SIZE(SMALL)

PEERDIR(
    ydb/library/actors/testlib
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/dq/actors/yt
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14

    ydb/library/yql/dq/integration/transform
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/minikql/comp_nodes
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
