LIBRARY()

SRCS(
    snapshot_retention.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/long_tx_service/public
)

END()

RECURSE_FOR_TESTS(
    ut
)
