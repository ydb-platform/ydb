LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/iam
    ydb/public/sdk/cpp/client/ydb_types/credentials
    ydb/library/yql/public/issue/protos
)

END()
