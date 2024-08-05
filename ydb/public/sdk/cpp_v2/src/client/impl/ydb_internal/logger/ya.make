LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
