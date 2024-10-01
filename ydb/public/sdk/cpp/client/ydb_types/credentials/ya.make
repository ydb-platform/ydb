LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    credentials.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
