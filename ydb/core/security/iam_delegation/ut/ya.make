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
    ydb/library/ycloud/impl
)

# The IamDelegationLive probes run against a real IAM installation and are compiled only on request:
#   ya make -tA --cflags=-DIAM_LIVE_TESTS ydb/core/security/iam_delegation/ut -F 'IamDelegationLive::*'
# with the environment described next to the suite.

YQL_LAST_ABI_VERSION()

SRCS(
    iam_delegation_ut.cpp
)

END()
