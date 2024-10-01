LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    operation.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/operation
)

END()
