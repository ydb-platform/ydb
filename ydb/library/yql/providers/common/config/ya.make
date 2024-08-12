LIBRARY()

SRCS(
    yql_configuration_transformer.cpp
    yql_dispatch.cpp
    yql_setting.h
)

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/ast
    ydb/library/yql/utils/log
    library/cpp/containers/sorted_vector
    library/cpp/string_utils/parse_size
    library/cpp/string_utils/levenshtein_diff
)


   YQL_LAST_ABI_VERSION()


END()
