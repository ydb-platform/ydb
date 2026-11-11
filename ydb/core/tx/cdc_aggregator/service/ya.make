LIBRARY()

SRCS(
    heartbeat_queue.cpp
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
