UNITTEST_FOR(ydb/mvp/meta/support_links)

SIZE(SMALL)

SRCS(
    support_links_config_validation_ut.cpp
)

PEERDIR(
    ydb/mvp/meta
    ydb/mvp/core
    ydb/core/testlib/actors
)

END()
