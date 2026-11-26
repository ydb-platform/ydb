LIBRARY()

SRCS(
    cdc_aggregator_heartbeat_broadcaster.cpp
    cdc_aggregator_heartbeat_partition_channel.cpp
    cdc_aggregator_heartbeat_tablet_pipe.cpp
    cdc_aggregator_stream.cpp
    cdc_aggregator_stream_proto.cpp
    cdc_aggregator_stream_repository.cpp
    tablet.cpp
    tx_configure_tracking.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_partitioning_changed.cpp
    tx_write_heartbeats.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx/cdc_aggregator/public
    ydb/core/tx/scheme_cache
    ydb/library/actors/core
    ydb/public/lib/base
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
