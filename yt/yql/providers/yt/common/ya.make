LIBRARY()

SRCS(
    yql_configuration.cpp
    yql_names.cpp
    yql_yt_settings.cpp
)

PEERDIR(
    library/cpp/regex/pcre
    library/cpp/string_utils/parse_size
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    yql/essentials/ast
    yql/essentials/utils/log
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/config
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_settings.h)

END()
