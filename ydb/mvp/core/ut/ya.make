UNITTEST_FOR(ydb/mvp/core)

OWNER(
    xenoxeno
    g:kikimr
)

SIZE(SMALL)

SRCS(
    mvp_ut.cpp
    mvp_tokens.cpp
    mvp_test_runtime.cpp
)

PEERDIR(
    ydb/core/testlib/actors
    contrib/libs/jwt-cpp
)

END()
