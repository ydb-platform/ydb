LIBRARY()

SRCS(
    api_utils.cpp
    json_utils.cpp
    interactive_config.cpp
    interactive_log.cpp
    line_reader.cpp
)

PEERDIR(
    contrib/restricted/patched/replxx
    library/cpp/json
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/string_utils/url
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yverify_stream
    ydb/public/lib/ydb_cli/commands/interactive/complete
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/common
    yql/essentials/sql/v1/complete
)

GENERATE_ENUM_SERIALIZATION(interactive_config.h)

END()
