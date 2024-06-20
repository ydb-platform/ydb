LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_public/iam/v1
    ydb/public/sdk/cpp/client/iam/impl
    ydb/public/sdk/cpp/client/iam/common
)

END()
