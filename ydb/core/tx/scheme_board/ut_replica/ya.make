UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/minikql/comp_nodes/llvm14
    ydb/core/testlib/basics/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    replica_ut.cpp
    ut_helpers.cpp
)

END()
