UNITTEST_FOR(ydb/mvp/meta)

SIZE(SMALL)

SRCS(
    meta_cache_ut.cpp
    meta_capabilities_ut.cpp
    meta_support_links_ut.cpp
    meta_ut.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/core/testlib/actors
)

END()
