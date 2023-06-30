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
    ydb/library/yql/ast
    ydb/library/yql/utils/log
    ydb/library/yql/providers/common/codec
    ydb/library/yql/providers/common/config
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_settings.h)

END()
