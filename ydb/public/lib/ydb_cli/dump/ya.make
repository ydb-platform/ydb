LIBRARY()

SRCS(
    dump.cpp
    dump_impl.cpp
    restore_impl.cpp
    restore_import_data.cpp
    restore_compat.cpp
)

PEERDIR(
    contrib/libs/re2
    library/cpp/bucket_quoter
    library/cpp/logger
    library/cpp/regex/pcre
    library/cpp/string_utils/quote
    ydb/library/backup
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump/files
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/sdk/cpp/src/client/query
    ydb/public/sdk/cpp/src/client/topic
)

GENERATE_ENUM_SERIALIZATION(dump.h)

END()
