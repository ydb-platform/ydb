LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    settings.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/common_client
)

END()
