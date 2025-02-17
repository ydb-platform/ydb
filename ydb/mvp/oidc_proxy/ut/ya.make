UNITTEST_FOR(ydb/mvp/oidc_proxy)

SIZE(SMALL)

SRCS(
    oidc_proxy_ut.cpp
    openid_connect.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/core/testlib/actors
    ydb/library/testlib/service_mocks
)

END()
