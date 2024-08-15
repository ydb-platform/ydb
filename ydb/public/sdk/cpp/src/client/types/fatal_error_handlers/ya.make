LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
