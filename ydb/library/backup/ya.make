LIBRARY(kikimr_backup)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/logger
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    yql/essentials/types/dynumber
    yql/essentials/sql/v1/format
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/lib/yson_value
    ydb/public/lib/ydb_cli/dump/files
    ydb/public/sdk/cpp/src/client/cms
    ydb/public/sdk/cpp/src/client/coordination
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/rate_limiter
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/value
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
