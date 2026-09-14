UNITTEST()

SRCS(
    operation_idempotency_ut.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/schemeshard/common
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/library/operation_id/protos
)

END()
