UNITTEST_FOR(ydb/core/kqp/workload_service)

FORK_SUBTESTS()

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    kqp_workload_service_actors_ut.cpp
    kqp_workload_service_tables_ut.cpp
    kqp_workload_service_query_sessions_ut.cpp
    kqp_workload_service_ut.cpp
)

PEERDIR(
    ydb/core/kqp/workload_service/ut/common

    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
