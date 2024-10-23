LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    result.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/result
)

END()
