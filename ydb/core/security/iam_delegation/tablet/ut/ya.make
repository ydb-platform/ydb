UNITTEST_FOR(ydb/core/security/iam_delegation/tablet)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/testlib/default
)

SRCS(
    iam_delegation_ut.cpp
    service_backoff_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
