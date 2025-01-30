LIBRARY()

SRCS(
    yql_configuration_transformer.cpp
    yql_dispatch.cpp
    yql_setting.h
)

PEERDIR(
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/ast
    yql/essentials/utils/log
    library/cpp/containers/sorted_vector
    library/cpp/string_utils/parse_size
    library/cpp/string_utils/levenshtein_diff
)


   YQL_LAST_ABI_VERSION()


END()
