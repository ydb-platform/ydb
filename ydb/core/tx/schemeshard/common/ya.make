LIBRARY()

SRCS(
    operation_idempotency.cpp
    operation_idempotency_support.cpp
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

RECURSE_FOR_TESTS(
    ut
)
