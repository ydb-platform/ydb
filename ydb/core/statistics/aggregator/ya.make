LIBRARY()

SRCS(
    aggregator.h
    aggregator.cpp
    aggregator_impl.h
    aggregator_impl.cpp
    schema.h
    schema.cpp
    tx_configure.cpp
    tx_delete_query_response.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_navigate.cpp
    tx_resolve.cpp
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
)

YQL_LAST_ABI_VERSION()

END()
