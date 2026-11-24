LIBRARY()

SRCS(
    cdc_aggregator_heartbeat_delivery.cpp
    cdc_aggregator_heartbeat_delivery_proto.cpp
    service.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx/cdc_aggregator/public
    ydb/library/actors/core
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
