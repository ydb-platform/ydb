UNITTEST_FOR(ydb/core/sys_view/service)

FORK_SUBTESTS()
SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
)

SRCS(
    db_counters_codec_ut.cpp
    query_history_ut.cpp
)

END()
