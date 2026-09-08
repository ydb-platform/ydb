UNITTEST_FOR(ydb/core/kqp/workload_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_workload_service_actors_ut.cpp
    kqp_workload_service_tables_ut.cpp
    kqp_workload_service_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util
    yql/essentials/sql/v1
    ydb/core/kqp/workload_service/ut/common

    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
