UNITTEST_FOR(ydb/mvp/meta)

SIZE(SMALL)

SRCS(
    resolver_factory_ut.cpp
    grafana_dashboard_resolver_ut.cpp
    grafana_dashboard_search_resolver_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
)

END()
