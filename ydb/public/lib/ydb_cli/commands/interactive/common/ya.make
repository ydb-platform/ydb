LIBRARY()

SRCS(
    api_utils.cpp
    json_utils.cpp
    interactive_config.cpp
    interactive_log.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/string_utils/url
    ydb/core/base
    ydb/public/lib/ydb_cli/common
)

GENERATE_ENUM_SERIALIZATION(interactive_config.h)

END()
