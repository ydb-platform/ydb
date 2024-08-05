LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    handlers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/types/exceptions
)

END()
