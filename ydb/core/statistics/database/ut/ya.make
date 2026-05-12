UNITTEST_FOR(ydb/core/statistics/database)

FORK_SUBTESTS()

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/statistics/ut_common
)

SRCS(
    ut_database.cpp
)

END()
