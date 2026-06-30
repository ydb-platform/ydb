LIBRARY()

SRCS(
    entity.cpp
    grafana_dashboard_common.cpp
    grafana_dashboard_search_source.cpp
    grafana_dashboard_source.cpp
    grafana_logging_source.cpp
    param_bindings.cpp
    response.cpp
    source.cpp
    support_links_resolver.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/meta/protos
    library/cpp/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
