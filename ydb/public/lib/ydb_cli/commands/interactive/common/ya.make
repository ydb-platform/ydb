LIBRARY()

SRCS(
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

# TODO: Use simpler HTTP library (e.g. curl wrapper) instead of http_gateway
# to avoid heavy dependencies (actors/interconnect -> crc32c -> crcutil with SSE4)
IF (NOT OS_WINDOWS OR USE_SSE4)
    SRCS(
        api_utils.cpp
    )
    PEERDIR(
        ydb/library/yql/providers/common/http_gateway
    )
    CFLAGS(-DYDB_CLI_AI_ENABLED=1)
ENDIF()

GENERATE_ENUM_SERIALIZATION(interactive_config.h)

END()
