LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    out.cpp
    value.cpp
)

GENERATE_ENUM_SERIALIZATION(ydb/public/sdk/cpp_v2/include/ydb-cpp-sdk/client/value/value.h)

PEERDIR(
    library/cpp/containers/stack_vector
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/value_helpers
    ydb/public/sdk/cpp_v2/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp_v2/src/library/yql/public/decimal
    ydb/public/sdk/cpp_v2/src/library/uuid
)

END()
