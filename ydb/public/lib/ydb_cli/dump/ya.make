LIBRARY()

SRCS(
    dump.cpp
    dump_impl.cpp
    restore_impl.cpp
    restore_import_data.cpp
    restore_compat.cpp
)

PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/string_utils/quote
    ydb/library/backup
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/ydb_cli/dump/util
    ydb/public/sdk/cpp/client/ydb_proto
)

END()
