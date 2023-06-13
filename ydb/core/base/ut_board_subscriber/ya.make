UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    library/cpp/actors/interconnect
    library/cpp/actors/core
    library/cpp/testing/unittest
    ydb/core/testlib/basics
    ydb/core/base
    ydb/core/testlib/basics/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    board_subscriber_ut.cpp
)

END()
