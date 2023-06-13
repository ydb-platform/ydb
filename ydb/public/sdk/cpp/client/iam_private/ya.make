LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_private/iam
    ydb/public/sdk/cpp/client/iam/common
)

END()
