LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
