LIBRARY()

SRCS(
    operation_idempotency.cpp
    validation.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/scheme_types
)

END()
