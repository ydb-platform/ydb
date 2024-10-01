LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    extension.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/extension_common
)

END()
