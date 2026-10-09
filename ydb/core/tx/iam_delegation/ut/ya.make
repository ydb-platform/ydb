UNITTEST_FOR(ydb/core/tx/iam_delegation)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    iam_delegation_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
