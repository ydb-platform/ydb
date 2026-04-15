UNITTEST_FOR(ydb/mvp/meta/support_links)

SIZE(SMALL)

SRCS(
    grafana_dashboard_source_ut.cpp
    grafana_dashboard_search_source_ut.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/meta
    ydb/core/testlib/actors
)

END()
