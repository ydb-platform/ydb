LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    accessor.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/proto
)

END()
