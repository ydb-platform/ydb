UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/library/actors/core
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    ydb/library/yql/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

SRCS(
    monitoring_ut.cpp
    ut_helpers.cpp
)

END()
