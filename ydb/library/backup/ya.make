LIBRARY(kikimr_backup)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/logger
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    ydb/library/dynumber
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_table
    ydb/public/sdk/cpp/client/ydb_value
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
