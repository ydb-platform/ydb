UNITTEST()

SIZE(SMALL)

SRCS(
    replication_utils_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/draft
    yql/essentials/public/udf
)

END()
