UNITTEST_FOR(ydb/core/tx/scheme_board)

OWNER(
    ilnaz
    g:kikimr
)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/actors/interconnect
    library/cpp/testing/unittest
    ydb/core/testlib/basics
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    subscriber_ut.cpp
    ut_helpers.cpp
)

END()
