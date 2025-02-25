UNITTEST_FOR(ydb/core/tx/scheme_board)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/library/actors/interconnect
    library/cpp/testing/unittest
    ydb/core/testlib/basics/default
    yql/essentials/minikql/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

SRCS(
    subscriber_ut.cpp
    ut_helpers.cpp
)

END()
