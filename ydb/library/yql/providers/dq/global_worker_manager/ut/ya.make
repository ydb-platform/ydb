UNITTEST_FOR(ydb/library/yql/providers/dq/global_worker_manager)

NO_BUILD_IF(OS_WINDOWS)

SIZE(SMALL)

PEERDIR(
    ydb/library/actors/testlib
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/providers/dq/actors/yt
    ydb/library/yql/providers/dq/actors
    ydb/library/yql/dq/actors/compute
)

SRCS(
    global_worker_manager_ut.cpp
    workers_storage_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/supp/ubsan_supp.inc)

YQL_LAST_ABI_VERSION()

END()
