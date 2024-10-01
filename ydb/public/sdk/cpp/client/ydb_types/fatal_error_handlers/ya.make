LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    handlers.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
)

END()
