LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    out.cpp
    proto_accessor.cpp
    result.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp_v2/src/client/value
    ydb/public/sdk/cpp_v2/src/client/proto
)

END()
