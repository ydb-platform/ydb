LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    params.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/params
)

END()
