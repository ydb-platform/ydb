LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    ydb/public/api/client/yc_public/iam
    ydb/public/sdk/cpp/client/iam/impl
    ydb/public/sdk/cpp/client/iam/common
)

END()
