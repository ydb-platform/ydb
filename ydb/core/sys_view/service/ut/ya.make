UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    db_counters_codec_ut.cpp
    query_history_ut.cpp
    sysview_service_ut.cpp
)

END()
