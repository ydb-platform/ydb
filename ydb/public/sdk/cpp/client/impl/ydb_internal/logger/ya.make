LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
