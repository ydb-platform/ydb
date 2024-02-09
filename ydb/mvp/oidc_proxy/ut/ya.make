UNITTEST_FOR(ydb/mvp/oidc_proxy)

OWNER(
    molotkov-and
    g:kikimr
)

SIZE(SMALL)

SRCS(
    oidc_proxy_ut.cpp
    openid_connect.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/core/testlib
    ydb/core/testlib/actors
)

END()
