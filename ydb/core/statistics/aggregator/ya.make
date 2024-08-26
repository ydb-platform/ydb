LIBRARY()

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    schema.h
    schema.cpp
    tx_ack_timeout.cpp
    tx_aggr_stat_response.cpp
    tx_analyze.cpp
    tx_analyze_table_delivery_problem.cpp
    tx_analyze_table_request.cpp
    tx_analyze_table_response.cpp
    tx_configure.cpp
    tx_datashard_scan_response.cpp
    tx_finish_trasersal.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_navigate.cpp
    tx_resolve.cpp
    tx_response_tablet_distribution.cpp
    tx_schedule_traversal.cpp
    tx_schemeshard_stats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/statistics/database
    ydb/library/minsketch
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
