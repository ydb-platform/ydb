UNITTEST_FOR(ydb/core/security/iam_delegation)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/testing/unittest
    ydb/core/kqp/common
    ydb/core/testlib/default
    ydb/library/testlib/service_mocks
)

YQL_LAST_ABI_VERSION()

SRCS(
    iam_delegation_ut.cpp
)

END()
