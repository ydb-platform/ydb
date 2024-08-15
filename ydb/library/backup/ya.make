LIBRARY(kikimr_backup)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    util
    ydb/library/dynumber
    ydb/public/api/grpc
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/library/yson_value
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/scheme
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    backup.cpp
    query_builder.cpp
    query_uploader.cpp
    util.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
