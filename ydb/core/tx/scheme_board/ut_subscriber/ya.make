UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/library/actors/interconnect
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    subscriber_ut.cpp
    ut_helpers.cpp
)

END()
