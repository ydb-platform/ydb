LIBRARY()

SRCS(
    iam.h
)

PEERDIR(
    ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/common
    ydb/public/sdk/cpp/src/client/iam/common
)

END()
