LIBRARY()

SRCS(
    operation_idempotency.cpp
    scheme_operation_idempotency.cpp
    validation.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/protos/schemeshard
    ydb/core/scheme
    ydb/core/scheme_types
    ydb/public/api/protos
)

END()
