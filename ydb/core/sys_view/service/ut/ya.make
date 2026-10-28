UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    ydb/core/tx/scheme_cache
    ydb/core/tablet_flat/test/libs/table
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    db_counters_codec_ut.cpp
    query_history_ut.cpp
    sysview_service_ut.cpp
    query_interval_ut.cpp
)

END()
