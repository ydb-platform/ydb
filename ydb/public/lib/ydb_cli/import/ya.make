LIBRARY()

SRCS(
    import.cpp
    cli_arrow_helpers.cpp
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/lib/ydb_cli/common
    ydb/public/sdk/cpp/src/client/proto
    ydb/public/lib/json_value
    contrib/libs/apache/arrow
    library/cpp/string_utils/csv
)

GENERATE_ENUM_SERIALIZATION(import.h)

END()
