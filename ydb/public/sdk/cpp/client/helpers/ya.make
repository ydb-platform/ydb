LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/iam/common
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/library/yql/public/issue/protos
)

END()
