UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tablet_flat/test/libs/table
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    query_history_ut.cpp
    query_interval_ut.cpp
    query_metrics_retention_db_ut.cpp
)

END()
