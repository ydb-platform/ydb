LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
