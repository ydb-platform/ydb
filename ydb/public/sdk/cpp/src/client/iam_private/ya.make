LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_private/iam
    ydb/public/sdk/cpp/src/client/iam_private/common
)

END()
