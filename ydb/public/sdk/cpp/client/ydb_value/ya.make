LIBRARY()

SRCS(
    out.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    ydb/public/api/protos
    ydb/public/sdk/cpp/client/impl/ydb_internal/value_helpers
    ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers
    ydb/library/yql/public/decimal
    ydb/library/uuid
)

END()

RECURSE_FOR_TESTS(
    ut
)
