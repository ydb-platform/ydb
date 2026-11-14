LIBRARY()

SRCS(
    stream.cpp
    tablet.cpp
    tx_configure_tracking.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_partitioning_changed.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/cdc_aggregator/public
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
