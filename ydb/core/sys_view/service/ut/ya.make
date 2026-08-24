UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

SRCS(
    query_history_ut.cpp
    sysview_service_ut.cpp
)

END()
