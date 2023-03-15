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
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_scheme
    ydb/public/sdk/cpp/client/ydb_table
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
