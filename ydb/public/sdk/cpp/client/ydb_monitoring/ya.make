LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    monitoring.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/monitoring
)

END()
