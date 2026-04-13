UNITTEST_FOR(ydb/mvp/oidc_proxy)

SIZE(SMALL)

SRCS(
    mvp_config_validation_ut.cpp
    oidc_proxy_ut.cpp
    openid_connect.cpp
    tokenator_integration_ut.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/core/testlib/actors
    ydb/library/testlib/service_mocks
)

END()
