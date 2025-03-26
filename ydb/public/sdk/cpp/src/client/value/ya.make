LIBRARY()

SRCS(
    out.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/value_helpers
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp/src/library/decimal
    ydb/public/sdk/cpp/src/library/uuid
)

END()
