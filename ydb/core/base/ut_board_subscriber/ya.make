UNITTEST_FOR(ydb/core/base)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/core
    library/cpp/testing/unittest
    ydb/core/testlib/basics
    ydb/core/base
    ydb/core/testlib/basics/default
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    board_subscriber_ut.cpp
)

END()
