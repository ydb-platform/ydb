UNITTEST_FOR(ydb/mvp/core)

SIZE(SMALL)

SRCS(
    mvp_ut.cpp
    mvp_tokens.cpp
    mvp_test_runtime.cpp
)

PEERDIR(
    ydb/core/testlib/actors
    contrib/libs/jwt-cpp
    ydb/library/testlib/service_mocks
)

END()
