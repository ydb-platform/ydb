LIBRARY()

SRCS(
    cdc_aggregator_heartbeat_publication.cpp
    cdc_aggregator_heartbeat_publisher.cpp
    tablet.cpp
    tx_init.cpp
    tx_init_schema.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/cdc_aggregator/public
    ydb/library/actors/core
    ydb/public/lib/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
