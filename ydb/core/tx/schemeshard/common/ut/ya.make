UNITTEST()

SRCS(
    operation_idempotency_ut.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard/common
    ydb/public/api/protos
)

END()
