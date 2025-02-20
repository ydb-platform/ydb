LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_private/iam/v1
    ydb/public/sdk/cpp/client/iam/common
)

END()
