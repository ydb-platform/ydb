LIBRARY()

SRCS(
    protocol.cpp
    stream_repository_over_local_db.cpp
    tablet.cpp
    tx_activate.cpp
    tx_configure.cpp
    tx_describe_stream.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_replace_source.cpp
    tx_report_delivered_batch.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/cdc_aggregator/public
    ydb/core/tx/cdc_aggregator/stream
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
