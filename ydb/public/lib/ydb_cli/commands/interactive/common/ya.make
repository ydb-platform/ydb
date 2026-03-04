LIBRARY()

SRCS(
    api_utils.cpp
    config_ui.cpp
    interactive_config.cpp
    interactive_settings.cpp
    json_utils.cpp
    line_reader.cpp
)

PEERDIR(
    contrib/libs/ftxui
    contrib/restricted/patched/replxx
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/url
    ydb/library/yverify_stream
    ydb/public/lib/ydb_cli/commands/interactive/complete
    ydb/public/lib/ydb_cli/commands/interactive/highlight
    ydb/public/lib/ydb_cli/common
    yql/essentials/sql/v1/complete
)

GENERATE_ENUM_SERIALIZATION(interactive_config.h)

END()
