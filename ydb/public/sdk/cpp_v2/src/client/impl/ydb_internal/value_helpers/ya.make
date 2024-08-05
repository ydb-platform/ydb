LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/client/types/fatal_error_handlers
)

END()
