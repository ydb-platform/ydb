LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    ydb/core/tx/cdc_aggregator/public
    ydb/library/actors/core
    ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
