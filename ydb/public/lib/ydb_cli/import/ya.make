LIBRARY()

SRCS(
    import.cpp
    cli_arrow_helpers.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp_v2/src/library/json_value
    contrib/libs/apache/arrow
    library/cpp/string_utils/csv
)

END()
