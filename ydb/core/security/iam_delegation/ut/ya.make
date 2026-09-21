UNITTEST_FOR(ydb/core/security/iam_delegation)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/server
    ydb/core/kqp/common
    ydb/core/testlib/default
    ydb/library/ycloud/impl
    ydb/library/testlib/service_mocks
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

SRCS(
    iam_delegation_ut.cpp
)

END()
