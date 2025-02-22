LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
