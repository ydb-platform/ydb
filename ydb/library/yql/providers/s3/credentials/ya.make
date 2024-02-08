LIBRARY()

SRCS(
    credentials.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/s3/proto
    ydb/public/sdk/cpp/client/ydb_types/credentials
)

END()

RECURSE_FOR_TESTS(
    ut
)
