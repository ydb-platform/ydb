UNITTEST_FOR(ydb/core/security/token_manager)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(20)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/util/actorsys_test
    ydb/core/protos
    ydb/library/actors/core
    ydb/library/actors/http
)

YQL_LAST_ABI_VERSION()

SRCS(
    token_manager_ut.cpp
)

END()
