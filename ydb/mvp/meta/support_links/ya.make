UNITTEST_FOR(ydb/mvp/meta)

SIZE(SMALL)

SRCS(
    grafana_dashboard_source_ut.cpp
    grafana_dashboard_search_source_ut.cpp
)

PEERDIR(
    ydb/library/actors/testlib
)

END()
