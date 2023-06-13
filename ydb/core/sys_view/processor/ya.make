LIBRARY()

SRCS(
    processor.h
    processor.cpp
    processor_impl.h
    processor_impl.cpp
    schema.h
    schema.cpp
    db_counters.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_configure.cpp
    tx_collect.cpp
    tx_aggregate.cpp
    tx_interval_summary.cpp
    tx_interval_metrics.cpp
    tx_top_partitions.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/grpc_services/counters
    ydb/core/kqp/counters
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()

END()
