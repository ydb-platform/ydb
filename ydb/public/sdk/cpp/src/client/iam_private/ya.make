LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_private/iam
    ydb/public/sdk/cpp/src/client/iam_private/common
)

END()
