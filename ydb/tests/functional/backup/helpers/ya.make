LIBRARY()

PEERDIR(
    ydb/library/testlib/s3_recipe_helper
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    backup_test_fixture.cpp
)

END()
