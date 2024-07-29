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
    tx_configure.cpp
    tx_delete_query_response.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_navigate.cpp
    tx_resolve.cpp
    tx_response_tablet_distribution.cpp
    tx_save_query_response.cpp
    tx_scan_table.cpp
    tx_schedule_scan.cpp
    tx_schemeshard_stats.cpp
    tx_statistics_scan_response.cpp
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
