LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    coordination.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/coordination
)

END()
