LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    value.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/value
)

END()
