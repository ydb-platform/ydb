LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    iam.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/iam/common
)

END()
