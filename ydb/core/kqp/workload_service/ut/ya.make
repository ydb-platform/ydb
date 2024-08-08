UNITTEST_FOR(ydb/core/kqp/workload_service)

FORK_SUBTESTS()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    kqp_workload_service_actors_ut.cpp
    kqp_workload_service_tables_ut.cpp
    kqp_workload_service_ut.cpp
)

PEERDIR(
    ydb/core/kqp/workload_service/ut/common

    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
