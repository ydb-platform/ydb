LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    discovery.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/discovery
)

END()
