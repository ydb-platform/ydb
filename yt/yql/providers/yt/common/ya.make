YQL_LIBRARY()

SRCS(
    yql_configuration.cpp
    yql_names.cpp
    yql_yt_settings.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    library/cpp/string_utils/parse_size
    library/cpp/yson/node
    library/cpp/json
    yt/cpp/mapreduce/interface
    yql/essentials/ast
    yql/essentials/utils/log
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/config
    yql/essentials/providers/common/provider
)

GENERATE_ENUM_SERIALIZATION(yql_yt_settings.h)

END()
