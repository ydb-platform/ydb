LIBRARY()

SRCS(
    yql_dispatch.cpp
    yql_setting.h
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/sql_types
    yql/essentials/ast
    yql/essentials/core/sql_types
    yql/essentials/utils/log
    yql/essentials/ast
    library/cpp/containers/sorted_vector
    library/cpp/string_utils/parse_size
    library/cpp/string_utils/levenshtein_diff
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(transformer)

RECURSE_FOR_TESTS(ut)
